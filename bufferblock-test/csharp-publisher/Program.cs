//******************************************************************************************************
//  Program.cs - STTP Interop BufferBlock Test Publisher
//
//  Sends `BufferBlockMeasurement`s with deterministic payloads so the pyapi-based subscriber on the
//  other side of the wire can verify byte-for-byte that pyapi correctly decodes gsfapi's
//  `ServerResponse.BufferBlock` (0x88) frame:
//
//      +0  uint32  sequenceNumber  (big-endian, per-publisher, monotonically increasing)
//      +4  byte    cacheIndex      (signal-index-cache selector)
//      +5  int32   signalIndex     (big-endian, runtime ID)
//      +9  byte[]  payload         (opaque)
//
//  The publisher emits three test patterns by default:
//      1. ASCII text       "HELLO BUFFERBLOCK"
//      2. Binary counter   bytes 0x00..0xFF
//      3. JSON event       {"signalid":"...","value":1.0,...}
//  The subscriber matches each received buffer against the expected byte stream.
//******************************************************************************************************

using System.Data;
using Gemstone;
using Gemstone.Configuration;
using Gemstone.Diagnostics;
using Gemstone.Timeseries;
using Microsoft.Extensions.Configuration;
using sttp;
using ConfigSettings = Gemstone.Configuration.Settings;

namespace SttpBufferBlockInteropTest;

internal class Program
{
    // Single test buffer-block signal - fixed signal id and metadata
    private static readonly Guid s_signalID = new("aabbccdd-1122-3344-5566-778899aabbcc");
    private const string PointTag = "TEST:BUFFERBLOCK";
    private const string DeviceAcronym = "BBTEST";
    private const string SignalReference = $"{DeviceAcronym}-CV1";
    private const string SignalAcronym = "CALC";
    private const ulong PointID = 1;

    private static readonly object s_outLock = new();
    private static int s_clientsConnected;
    private static int s_publishCount;
    private static bool s_requireConfirmation = true;

    private static void Status(string message)
    {
        lock (s_outLock)
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
    }

    private static int Main(string[] args)
    {
        ushort port = 7175;
        bool autoPublish = false;
        int autoCount = 3;
        double autoDelaySeconds = 5.0;
        bool allowTssc = true;
        bool requireConfirmation = true;

        foreach (string arg in args)
        {
            if (arg.Equals("--auto", StringComparison.OrdinalIgnoreCase))
                autoPublish = true;
            else if (arg.StartsWith("--auto-count=", StringComparison.OrdinalIgnoreCase))
                autoCount = int.Parse(arg["--auto-count=".Length..]);
            else if (arg.StartsWith("--auto-delay=", StringComparison.OrdinalIgnoreCase))
                autoDelaySeconds = double.Parse(arg["--auto-delay=".Length..]);
            else if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase))
                port = ushort.Parse(arg["--port=".Length..]);
            else if (arg.Equals("--no-tssc", StringComparison.OrdinalIgnoreCase))
                allowTssc = false;
            else if (arg.Equals("--no-confirm", StringComparison.OrdinalIgnoreCase))
                requireConfirmation = false;
            else if (arg.StartsWith("--help", StringComparison.OrdinalIgnoreCase))
            {
                PrintUsage();
                return 0;
            }
        }

        s_requireConfirmation = requireConfirmation;

        Status("=== STTP BufferBlock Interop Test Publisher ===");
        Status($"Port:               {port}");
        Status($"Test SignalID:      {s_signalID:D}");
        Status($"Test PointTag:      {PointTag}");
        Status($"Auto-publish:       {autoPublish}  (count={autoCount}, delay={autoDelaySeconds:F1}s)");
        Status("");

        InitializeGemstoneSettings();

        DataSet metadata = BuildMinimalMetadata();
        InMemoryMetadataDataPublisher publisher = new();

        publisher.StatusMessage += (_, e) => Status($"[PUB] {e.Argument}");
        publisher.ProcessException += (_, e) => Status($"[PUB][ERR] {e.Argument.Message}");
        publisher.ClientConnected += Publisher_ClientConnected;

        publisher.Name = "BB_TEST_PUBLISHER";
        publisher.ID = 1u;

        // Phase 0b fix: BufferBlocks now flow correctly through the TSSC path too. When this is
        // true, the subscriber sees buffer-block payloads GZip-compressed (Compressed flag set);
        // when false, they're sent raw. Both paths are exercised by toggling --no-tssc on the cmd line.
        publisher.AllowPayloadCompression = allowTssc;
        publisher.AllowMetadataRefresh = true;
        publisher.UseBaseTimeOffsets = true;
        publisher.DataSource = metadata;

        publisher.MetadataTables =
            "SELECT UniqueID, OriginalSource, IsConcentrator, Acronym, Name, AccessID, ParentAcronym, CompanyAcronym, VendorAcronym, VendorDeviceName, Longitude, Latitude, InterconnectionName, ContactList, Enabled, UpdatedOn FROM DeviceDetail WHERE IsConcentrator = 0;" +
            "SELECT DeviceAcronym, ID, SignalID, PointTag, AlternateTag, SignalReference, SignalAcronym, PhasorSourceIndex, Description, Internal, Enabled, UpdatedOn FROM MeasurementDetail;" +
            "SELECT ID, DeviceAcronym, Label, Type, Phase, SourceIndex, UpdatedOn FROM PhasorDetail;" +
            "SELECT VersionNumber FROM SchemaVersion";

        publisher.ConnectionString = $"commandChannel={{port={port}}}";

        publisher.Initialize();
        publisher.Start();

        Status($"Publisher started, listening on port {port}.");
        Status("Interactive: B+Enter publishes the 3-buffer-block test set, Enter exits.");
        Status("");

        CancellationTokenSource cts = new();

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            Status("Ctrl+C received - shutting down.");
            cts.Cancel();
        };

        if (autoPublish)
        {
            _ = Task.Run(async () =>
            {
                Status($"  --auto: waiting for subscriber to connect...");

                while (s_clientsConnected == 0 && !cts.Token.IsCancellationRequested)
                {
                    try { await Task.Delay(200, cts.Token); } catch (OperationCanceledException) { return; }
                }

                if (cts.Token.IsCancellationRequested)
                    return;

                Status($"  --auto: subscriber connected, waiting {autoDelaySeconds:F1}s before sending buffer block test set...");

                try { await Task.Delay(TimeSpan.FromSeconds(autoDelaySeconds), cts.Token); }
                catch (OperationCanceledException) { return; }

                for (int i = 0; i < autoCount && !cts.Token.IsCancellationRequested; i++)
                {
                    Status($"  --auto: sending buffer-block test set #{i + 1} of {autoCount}");
                    PublishBufferBlockTestSet(publisher);

                    try { await Task.Delay(TimeSpan.FromSeconds(2), cts.Token); }
                    catch (OperationCanceledException) { return; }
                }

                Status("  --auto: complete; idling 15s so subscriber can drain and we can observe");
                try { await Task.Delay(TimeSpan.FromSeconds(15), cts.Token); }
                catch (OperationCanceledException) { return; }

                cts.Cancel();
            }, cts.Token);
        }

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

                if (upper == "B")
                    PublishBufferBlockTestSet(publisher);
                else if (upper == "")
                    break;
                else
                    Status("Commands: B = publish the 3-buffer-block test set, Enter = exit");
            }
        }

        cts.Cancel();
        Status("Stopping publisher...");
        publisher.Stop();
        publisher.Dispose();
        Status("Publisher stopped.");
        return 0;
    }

    private static void InitializeGemstoneSettings()
    {
        ConfigSettings settings = new()
        {
            SQLite = ConfigurationOperation.Disabled,
            INIFile = ConfigurationOperation.Disabled
        };

        Gemstone.Timeseries.Adapters.IaonSession.DefineSettings(settings, ConfigSettings.SystemSettingsCategory);

        settings.Bind(new ConfigurationBuilder().ConfigureGemstoneDefaults(settings));
    }

    private static void PrintUsage()
    {
        Console.WriteLine("Usage: SttpBufferBlockInteropPublisher [--port=NNNN] [--auto [--auto-count=N] [--auto-delay=N]]");
        Console.WriteLine();
        Console.WriteLine("  --port=NNNN       TCP port to listen on (default 7175)");
        Console.WriteLine("  --auto            Auto-publish the 3-buffer-block test set after subscriber connects");
        Console.WriteLine("  --auto-count=N    Number of times to send the test set (default 3)");
        Console.WriteLine("  --auto-delay=N    Seconds to wait after subscriber connects before sending (default 5)");
        Console.WriteLine("  --no-tssc         Force the non-TSSC measurement path (BufferBlock payload sent uncompressed)");
        Console.WriteLine("  --no-confirm      Clear REQUIRE CONFIRMATION on each buffer block (fire-and-forget)");
        Console.WriteLine();
        Console.WriteLine("Interactive: B + Enter publishes the 3-buffer-block test set.");
    }

    /// <summary>
    /// The deterministic test set. Three buffer blocks, each with a known payload that the
    /// subscriber side can match exactly. Adjust the contents here AND in the subscriber side
    /// together when extending the test.
    /// </summary>
    private static readonly (string Label, byte[] Payload)[] s_testSet =
    [
        ("ASCII",   "HELLO BUFFERBLOCK"u8.ToArray()),
        ("BINARY",  Enumerable.Range(0, 256).Select(i => (byte)i).ToArray()),
        ("JSON",    System.Text.Encoding.UTF8.GetBytes("""{"signalid":"aabbccdd-1122-3344-5566-778899aabbcc","value":1.0,"type":"test-event"}""")),
    ];

    private static void PublishBufferBlockTestSet(DataPublisher publisher)
    {
        MeasurementKey key = MeasurementKey.LookUpOrCreate(s_signalID, $"PPA:{PointID}");

        List<IMeasurement> bufferBlocks = [];

        foreach ((string label, byte[] payload) in s_testSet)
        {
            s_publishCount++;
            BufferBlockMeasurement bb = new(payload, 0, payload.Length)
            {
                Metadata = key.Metadata,
                Timestamp = DateTime.UtcNow.Ticks,
                RequireConfirmation = s_requireConfirmation,
            };
            Status($"  >> PUBLISHING buffer-block #{s_publishCount} ({label}, {payload.Length} bytes, requireConfirmation={s_requireConfirmation})");
            bufferBlocks.Add(bb);
        }

        publisher.QueueMeasurementsForProcessing(bufferBlocks);
    }

    private static void Publisher_ClientConnected(object? sender, EventArgs<Guid, string, string> e)
    {
        Interlocked.Increment(ref s_clientsConnected);
        Status($"<< CLIENT CONNECTED: id={e.Argument1} info=\"{e.Argument3}\"");
    }

    private sealed class InMemoryMetadataDataPublisher : DataPublisher
    {
        protected override DataSet AcquireMetadata(SubscriberConnection connection, Dictionary<string, Tuple<string, string, int>> filterExpressions)
        {
            DataSet metadata = new();

            if (DataSource is null)
                return metadata;

            foreach (DataTable srcTable in DataSource.Tables)
            {
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

    private static DataSet BuildMinimalMetadata()
    {
        DataSet ds = new("Metadata");

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
        row["Description"] = "Buffer block interop test measurement";
        row["UpdatedOn"] = DateTime.UtcNow;
        active.Rows.Add(row);

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
        drow["Name"] = "Buffer Block Test Device";
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
        mrow["Description"] = "Buffer block interop test measurement";
        mrow["Internal"] = true;
        mrow["Enabled"] = true;
        mrow["UpdatedOn"] = DateTime.UtcNow;
        mdetail.Rows.Add(mrow);

        DataTable phasors = ds.Tables.Add("PhasorDetail");
        phasors.Columns.Add("ID", typeof(int));
        phasors.Columns.Add("DeviceAcronym", typeof(string));
        phasors.Columns.Add("Label", typeof(string));
        phasors.Columns.Add("Type", typeof(string));
        phasors.Columns.Add("Phase", typeof(string));
        phasors.Columns.Add("SourceIndex", typeof(int));
        phasors.Columns.Add("UpdatedOn", typeof(DateTime));

        DataTable schemaVer = ds.Tables.Add("SchemaVersion");
        schemaVer.Columns.Add("VersionNumber", typeof(int));
        DataRow svrow = schemaVer.NewRow();
        svrow["VersionNumber"] = 19;
        schemaVer.Rows.Add(svrow);

        return ds;
    }
}
