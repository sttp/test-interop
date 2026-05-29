//******************************************************************************************************
//  Program.cs - STTP Interop Single-Measurement Test Publisher
//
//  Test harness to investigate "missing single measurement" issue between gsfapi (C# publisher) and
//  pyapi (Python subscriber).
//
//  Theory: When sending a single measurement (e.g. an event/alarm), the gsfapi publisher's TSSC
//  compression engine may not flush the measurement to the wire. Disabling TSSC (toggle via -no-tssc)
//  should bypass the compression engine and use uncompressed CompactMeasurement format instead.
//******************************************************************************************************

using System.Data;
using Gemstone;
using Gemstone.Configuration;
using Gemstone.Diagnostics;
using Gemstone.Timeseries;
using Microsoft.Extensions.Configuration;
using sttp;
using ConfigSettings = Gemstone.Configuration.Settings;

namespace SttpInteropTest;

internal class Program
{
    // Single test measurement - fixed signal id and metadata
    private static readonly Guid s_signalID = new("12345678-1234-1234-1234-123456789ABC");
    private const string PointTag = "TEST:SINGLE_MEASUREMENT";
    private const string DeviceAcronym = "TESTDEV";
    private const string SignalReference = $"{DeviceAcronym}-CV1";
    private const string SignalAcronym = "CALC"; // Generic calculated value
    private const ulong PointID = 1;

    private static readonly object s_outLock = new();
    private static int s_clientsConnected;
    private static int s_publishCount;

    private static void Status(string message)
    {
        lock (s_outLock)
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
    }

    private static int Main(string[] args)
    {
        // Parse args
        ushort port = 7165;
        bool allowTssc = true;
        bool autoPublish = false;
        bool publishOnce = false;
        double onceDelaySeconds = 5.0;

        foreach (string arg in args)
        {
            if (arg.Equals("--no-tssc", StringComparison.OrdinalIgnoreCase))
                allowTssc = false;
            else if (arg.Equals("--auto", StringComparison.OrdinalIgnoreCase))
                autoPublish = true;
            else if (arg.Equals("--once", StringComparison.OrdinalIgnoreCase))
                publishOnce = true;
            else if (arg.StartsWith("--once-delay=", StringComparison.OrdinalIgnoreCase))
                onceDelaySeconds = double.Parse(arg.Substring("--once-delay=".Length));
            else if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase))
                port = ushort.Parse(arg.Substring("--port=".Length));
            else if (arg.StartsWith("--help", StringComparison.OrdinalIgnoreCase))
            {
                PrintUsage();
                return 0;
            }
        }

        Status($"=== STTP Single-Measurement Test Publisher ===");
        Status($"Port:               {port}");
        Status($"AllowPayloadCompression (TSSC): {allowTssc}");
        Status($"Auto-publish on connect:        {autoPublish}");
        Status($"Test SignalID:      {s_signalID:D}");
        Status($"Test PointTag:      {PointTag}");
        Status("");

        // Initialize Gemstone configuration so Logger / DataPublisher static initializers can run
        InitializeGemstoneSettings();

        // Build a minimal in-memory metadata DataSet
        DataSet metadata = BuildMinimalMetadata();

        // Create and configure the publisher
        InMemoryMetadataDataPublisher publisher = new();

        publisher.StatusMessage += (_, e) => Status($"[PUB] {e.Argument}");
        publisher.ProcessException += (_, e) => Status($"[PUB][ERR] {e.Argument.Message}");
        publisher.ClientConnected += Publisher_ClientConnected;

        publisher.Name = "TEST_PUBLISHER";
        publisher.ID = 1u;
        publisher.AllowPayloadCompression = allowTssc;          // Critical toggle for the test
        publisher.AllowMetadataRefresh = true;
        publisher.UseBaseTimeOffsets = true;
        publisher.DataSource = metadata;

        // Override default MetadataTables SQL to match the schema we actually built
        // (default queries reference VersionInfo, PhasorDetail.PrimaryVoltageID, etc.)
        publisher.MetadataTables =
            "SELECT UniqueID, OriginalSource, IsConcentrator, Acronym, Name, AccessID, ParentAcronym, CompanyAcronym, VendorAcronym, VendorDeviceName, Longitude, Latitude, InterconnectionName, ContactList, Enabled, UpdatedOn FROM DeviceDetail WHERE IsConcentrator = 0;" +
            "SELECT DeviceAcronym, ID, SignalID, PointTag, AlternateTag, SignalReference, SignalAcronym, PhasorSourceIndex, Description, Internal, Enabled, UpdatedOn FROM MeasurementDetail;" +
            "SELECT ID, DeviceAcronym, Label, Type, Phase, SourceIndex, UpdatedOn FROM PhasorDetail;" +
            "SELECT VersionNumber FROM SchemaVersion";

        publisher.ConnectionString = $"commandChannel={{port={port}}}";

        publisher.Initialize();
        publisher.Start();

        Status($"Publisher started, listening on port {port}.");
        Status("Waiting for subscriber to connect...");
        Status("Interactive: P+Enter publishes 1, PP+Enter publishes 10, Enter exits.");
        Status("Non-interactive: combine with --auto, or send SIGINT (Ctrl+C) to stop.");
        Status("");

        // Auto-publish loop in background if requested
        CancellationTokenSource cts = new();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Status("Ctrl+C received — shutting down.");
            cts.Cancel();
        };

        if (autoPublish)
        {
            _ = Task.Run(async () =>
            {
                int attempt = 0;
                while (!cts.Token.IsCancellationRequested)
                {
                    try { await Task.Delay(2000, cts.Token); } catch (OperationCanceledException) { break; }

                    if (s_clientsConnected > 0)
                    {
                        attempt++;
                        Status($"  -- auto-publish attempt #{attempt} --");
                        PublishSingleMeasurement(publisher);
                    }
                    else
                    {
                        Status("  (no clients connected, skipping auto-publish)");
                    }
                }
            }, cts.Token);
        }
        else if (publishOnce)
        {
            // Wait for client connect, then send exactly ONE measurement after a delay,
            // then idle (this mirrors the user's reported scenario in WaveAppsSample - a single
            // event/alarm measurement issued occasionally).
            _ = Task.Run(async () =>
            {
                Status($"  --once: waiting for subscriber to connect...");

                while (s_clientsConnected == 0 && !cts.Token.IsCancellationRequested)
                {
                    try { await Task.Delay(200, cts.Token); } catch (OperationCanceledException) { return; }
                }

                if (cts.Token.IsCancellationRequested)
                    return;

                Status($"  --once: subscriber connected, waiting {onceDelaySeconds:F1}s before sending one measurement...");

                try { await Task.Delay(TimeSpan.FromSeconds(onceDelaySeconds), cts.Token); }
                catch (OperationCanceledException) { return; }

                Status("  --once: sending the SINGLE measurement now");
                PublishSingleMeasurement(publisher);

                Status("  --once: measurement queued; staying idle for 30s so subscriber can process and we can observe");

                try { await Task.Delay(TimeSpan.FromSeconds(30), cts.Token); }
                catch (OperationCanceledException) { return; }

                Status("  --once: 30s idle complete, exiting");
                cts.Cancel();
            }, cts.Token);
        }

        // Try interactive read; if non-TTY, wait on cancellation
        if (Console.IsInputRedirected)
        {
            Status("(stdin redirected; waiting for cancel)");
            cts.Token.WaitHandle.WaitOne();
        }
        else
        {
            while (!cts.Token.IsCancellationRequested)
            {
                string? line = Console.ReadLine();
                if (line is null)
                    break;

                string upper = line.Trim().ToUpperInvariant();

                if (upper == "P")
                {
                    PublishSingleMeasurement(publisher);
                }
                else if (upper == "PP")
                {
                    // Burst test - send 10 measurements rapidly
                    for (int i = 0; i < 10; i++)
                        PublishSingleMeasurement(publisher);
                }
                else if (upper == "")
                {
                    break;
                }
                else
                {
                    Status("Commands: P = publish 1, PP = publish 10 burst, Enter = exit");
                }
            }
        }

        cts.Cancel();
        Status("Stopping publisher...");
        publisher.Stop();
        publisher.Dispose();
        Status("Publisher stopped.");
        return 0;
    }

    /// <summary>
    /// Minimal Gemstone settings initialization - required by Logger / StatisticsEngine and other
    /// static initializers in Gemstone.Diagnostics / Gemstone.Timeseries, etc., when running
    /// outside an IAONSession host. We call IaonSession.DefineSettings to populate the default
    /// settings the engine reads during static construction.
    /// </summary>
    private static void InitializeGemstoneSettings()
    {
        ConfigSettings settings = new()
        {
            SQLite = ConfigurationOperation.Disabled,
            INIFile = ConfigurationOperation.Disabled
        };

        // Define settings for Time-Series components (Statistics, IaonSession, etc.)
        Gemstone.Timeseries.Adapters.IaonSession.DefineSettings(settings, ConfigSettings.SystemSettingsCategory);

        settings.Bind(new ConfigurationBuilder().ConfigureGemstoneDefaults(settings));
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: SttpInteropPublisher [--port=NNNN] [--no-tssc] [--auto | --once [--once-delay=N]]");
        Console.WriteLine();
        Console.WriteLine("  --port=NNNN       TCP port to listen on (default 7165)");
        Console.WriteLine("  --no-tssc         Disable TSSC compression (forces CompactMeasurement)");
        Console.WriteLine("  --auto            Auto-publish one measurement every 2s after subscriber connects");
        Console.WriteLine("  --once            Publish exactly ONE measurement after a delay, then idle");
        Console.WriteLine("  --once-delay=N    Seconds to wait after subscriber connects before sending (default 5)");
        Console.WriteLine();
        Console.WriteLine("Interactive: P + Enter publishes one measurement, PP + Enter sends a 10-burst.");
    }

    private static void PublishSingleMeasurement(DataPublisher publisher)
    {
        s_publishCount++;
        long ticks = DateTime.UtcNow.Ticks;
        double value = 60.0 + (Random.Shared.NextDouble() - 0.5) * 0.05;

        Measurement measurement = new()
        {
            Metadata = MeasurementKey.LookUpOrCreate(s_signalID, $"PPA:{PointID}").Metadata,
            Timestamp = ticks,
            Value = value,
            StateFlags = MeasurementStateFlags.Normal
        };

        Status($"  >> PUBLISHING measurement #{s_publishCount}: ts={new DateTime(ticks):HH:mm:ss.fffffff} value={value:F6}");
        publisher.QueueMeasurementsForProcessing(new[] { (IMeasurement)measurement });
    }

    private static void Publisher_ClientConnected(object? sender, EventArgs<Guid, string, string> e)
    {
        Interlocked.Increment(ref s_clientsConnected);
        Status($"<< CLIENT CONNECTED: id={e.Argument1} info=\"{e.Argument3}\"");
    }

    /// <summary>
    /// DataPublisher that serves metadata refresh requests from an in-memory DataSet (DataSource)
    /// rather than from an ADO database. Required because the default <see cref="DataPublisher.AcquireMetadata"/>
    /// opens an <c>AdoDataConnection</c> from <c>ConfigSettings.Default</c>, which we don't have.
    /// </summary>
    private sealed class InMemoryMetadataDataPublisher : DataPublisher
    {
        protected override DataSet AcquireMetadata(SubscriberConnection connection, Dictionary<string, Tuple<string, string, int>> filterExpressions)
        {
            DataSet metadata = new();

            if (DataSource is null)
                return metadata;

            // Apply filter expressions to in-memory tables (default impl uses SQL against the ADO db)
            foreach (DataTable srcTable in DataSource.Tables)
            {
                // Only process tables we'd normally publish for metadata refresh
                if (srcTable.TableName == "ActiveMeasurements")
                    continue;

                if (filterExpressions.TryGetValue(srcTable.TableName, out Tuple<string, string, int>? filter))
                {
                    DataTable copy = srcTable.Clone();
                    DataRow[] selected = srcTable.Select(filter.Item1, filter.Item2);
                    int taken = 0;

                    foreach (DataRow row in selected)
                    {
                        if (taken++ >= filter.Item3) break;
                        copy.ImportRow(row);
                    }

                    metadata.Tables.Add(copy);
                }
                else
                {
                    metadata.Tables.Add(srcTable.Copy());
                }
            }

            return metadata;
        }
    }

    /// <summary>
    /// Builds the absolute minimum DataSet metadata required by DataPublisher to authorize and publish
    /// a single measurement. Only the ActiveMeasurements, MeasurementDetail, DeviceDetail tables are
    /// constructed (others such as PhasorDetail / SchemaVersion are added with one row to satisfy
    /// MetadataTables refresh requests).
    /// </summary>
    private static DataSet BuildMinimalMetadata()
    {
        DataSet ds = new("Metadata");

        // ---- ActiveMeasurements (used by FILTER expressions and signal authorization) ----
        DataTable active = ds.Tables.Add("ActiveMeasurements");
        active.Columns.Add("SourceNodeID", typeof(Guid));
        active.Columns.Add("ID", typeof(string));
        active.Columns.Add("SignalID", typeof(Guid));
        active.Columns.Add("PointTag", typeof(string));
        active.Columns.Add("AlternateTag", typeof(string));
        active.Columns.Add("SignalReference", typeof(string));
        active.Columns.Add("Internal", typeof(int));
        active.Columns.Add("Subscribed", typeof(int));
        active.Columns.Add("Device", typeof(string));
        active.Columns.Add("DeviceID", typeof(int));
        active.Columns.Add("FramesPerSecond", typeof(int));
        active.Columns.Add("Protocol", typeof(string));
        active.Columns.Add("ProtocolType", typeof(string));
        active.Columns.Add("SignalType", typeof(string));
        active.Columns.Add("EngineeringUnits", typeof(string));
        active.Columns.Add("PhasorID", typeof(int));
        active.Columns.Add("PhasorType", typeof(string));
        active.Columns.Add("Phase", typeof(string));
        active.Columns.Add("Adder", typeof(double));
        active.Columns.Add("Multiplier", typeof(double));
        active.Columns.Add("Company", typeof(string));
        active.Columns.Add("Longitude", typeof(decimal));
        active.Columns.Add("Latitude", typeof(decimal));
        active.Columns.Add("Description", typeof(string));
        active.Columns.Add("UpdatedOn", typeof(DateTime));

        DataRow row = active.NewRow();
        row["SourceNodeID"] = Guid.Empty;
        row["ID"] = $"PPA:{PointID}";
        row["SignalID"] = s_signalID;
        row["PointTag"] = PointTag;
        row["AlternateTag"] = "";
        row["SignalReference"] = SignalReference;
        row["Internal"] = 1;
        row["Subscribed"] = 0;
        row["Device"] = DeviceAcronym;
        row["DeviceID"] = 1;
        row["FramesPerSecond"] = 30;
        row["Protocol"] = "STTP";
        row["ProtocolType"] = "Measurement";
        row["SignalType"] = SignalAcronym;
        row["EngineeringUnits"] = "";
        row["PhasorID"] = DBNull.Value;
        row["PhasorType"] = DBNull.Value;
        row["Phase"] = DBNull.Value;
        row["Adder"] = 0.0;
        row["Multiplier"] = 1.0;
        row["Company"] = "TEST";
        row["Longitude"] = (decimal)0.0;
        row["Latitude"] = (decimal)0.0;
        row["Description"] = "Single test measurement for STTP interop";
        row["UpdatedOn"] = DateTime.UtcNow;
        active.Rows.Add(row);

        // ---- DeviceDetail (used by metadata refresh) ----
        DataTable devices = ds.Tables.Add("DeviceDetail");
        devices.Columns.Add("NodeID", typeof(Guid));
        devices.Columns.Add("UniqueID", typeof(Guid));
        devices.Columns.Add("OriginalSource", typeof(string));
        devices.Columns.Add("IsConcentrator", typeof(bool));
        devices.Columns.Add("Acronym", typeof(string));
        devices.Columns.Add("Name", typeof(string));
        devices.Columns.Add("AccessID", typeof(int));
        devices.Columns.Add("ParentAcronym", typeof(string));
        devices.Columns.Add("ProtocolName", typeof(string));
        devices.Columns.Add("FramesPerSecond", typeof(int));
        devices.Columns.Add("CompanyAcronym", typeof(string));
        devices.Columns.Add("VendorAcronym", typeof(string));
        devices.Columns.Add("VendorDeviceName", typeof(string));
        devices.Columns.Add("Longitude", typeof(decimal));
        devices.Columns.Add("Latitude", typeof(decimal));
        devices.Columns.Add("InterconnectionName", typeof(string));
        devices.Columns.Add("ContactList", typeof(string));
        devices.Columns.Add("Enabled", typeof(bool));
        devices.Columns.Add("UpdatedOn", typeof(DateTime));

        DataRow drow = devices.NewRow();
        drow["NodeID"] = Guid.Empty;
        drow["UniqueID"] = Guid.NewGuid();
        drow["OriginalSource"] = "";
        drow["IsConcentrator"] = false;
        drow["Acronym"] = DeviceAcronym;
        drow["Name"] = "Test Device";
        drow["AccessID"] = 0;
        drow["ParentAcronym"] = "";
        drow["ProtocolName"] = "STTP";
        drow["FramesPerSecond"] = 30;
        drow["CompanyAcronym"] = "TEST";
        drow["VendorAcronym"] = "";
        drow["VendorDeviceName"] = "";
        drow["Longitude"] = (decimal)0.0;
        drow["Latitude"] = (decimal)0.0;
        drow["InterconnectionName"] = "";
        drow["ContactList"] = "";
        drow["Enabled"] = true;
        drow["UpdatedOn"] = DateTime.UtcNow;
        devices.Rows.Add(drow);

        // ---- MeasurementDetail (used by metadata refresh) ----
        DataTable mdetail = ds.Tables.Add("MeasurementDetail");
        mdetail.Columns.Add("NodeID", typeof(Guid));
        mdetail.Columns.Add("DeviceAcronym", typeof(string));
        mdetail.Columns.Add("ID", typeof(string));
        mdetail.Columns.Add("SignalID", typeof(Guid));
        mdetail.Columns.Add("PointTag", typeof(string));
        mdetail.Columns.Add("AlternateTag", typeof(string));
        mdetail.Columns.Add("SignalReference", typeof(string));
        mdetail.Columns.Add("SignalAcronym", typeof(string));
        mdetail.Columns.Add("PhasorSourceIndex", typeof(int));
        mdetail.Columns.Add("Description", typeof(string));
        mdetail.Columns.Add("Internal", typeof(bool));
        mdetail.Columns.Add("Enabled", typeof(bool));
        mdetail.Columns.Add("UpdatedOn", typeof(DateTime));

        DataRow mrow = mdetail.NewRow();
        mrow["NodeID"] = Guid.Empty;
        mrow["DeviceAcronym"] = DeviceAcronym;
        mrow["ID"] = $"PPA:{PointID}";
        mrow["SignalID"] = s_signalID;
        mrow["PointTag"] = PointTag;
        mrow["AlternateTag"] = "";
        mrow["SignalReference"] = SignalReference;
        mrow["SignalAcronym"] = SignalAcronym;
        mrow["PhasorSourceIndex"] = DBNull.Value;
        mrow["Description"] = "Single test measurement for STTP interop";
        mrow["Internal"] = true;
        mrow["Enabled"] = true;
        mrow["UpdatedOn"] = DateTime.UtcNow;
        mdetail.Rows.Add(mrow);

        // ---- PhasorDetail (empty placeholder) ----
        DataTable phasors = ds.Tables.Add("PhasorDetail");
        phasors.Columns.Add("ID", typeof(int));
        phasors.Columns.Add("DeviceAcronym", typeof(string));
        phasors.Columns.Add("Label", typeof(string));
        phasors.Columns.Add("Type", typeof(string));
        phasors.Columns.Add("Phase", typeof(string));
        phasors.Columns.Add("SourceIndex", typeof(int));
        phasors.Columns.Add("UpdatedOn", typeof(DateTime));

        // ---- SchemaVersion ----
        DataTable schemaVer = ds.Tables.Add("SchemaVersion");
        schemaVer.Columns.Add("VersionNumber", typeof(int));
        DataRow svrow = schemaVer.NewRow();
        svrow["VersionNumber"] = 19;
        schemaVer.Rows.Add(svrow);

        return ds;
    }
}
