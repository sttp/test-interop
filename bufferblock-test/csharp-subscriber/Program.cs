//******************************************************************************************************
//  Program.cs - STTP Interop BufferBlock Test Subscriber (gsfapi side)
//
//  Connects to the pyapi-based publisher in test-interop/bufferblock/python-publisher and verifies
//  each `BufferBlockMeasurement` payload byte-for-byte against the expected test set.
//
//  Test set (must mirror python-publisher/main.py TEST_SET):
//      1. ASCII   "HELLO FROM PY"
//      2. BINARY  bytes 0xFF..0x00 (reverse counter)
//      3. JSON    UTF-8 JSON document
//
//  Exits with status 0 when --expect-sets is reached with no mismatches, 1 on any byte mismatch,
//  2 on timeout before all expected buffer blocks arrived.
//******************************************************************************************************

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
    private static readonly object s_outLock = new();

    // Pyapi test set - matches python-publisher/main.py TEST_SET
    private static readonly (string Label, byte[] Payload)[] s_pyapiPayloads =
    [
        ("ASCII",  "HELLO FROM PY"u8.ToArray()),
        ("BINARY", Enumerable.Range(0, 256).Reverse().Select(i => (byte)i).ToArray()),
        ("JSON",   System.Text.Encoding.UTF8.GetBytes("""{"source":"pyapi","value":42.0,"type":"buffer-block-test"}""")),
    ];

    // gsfapi test set - matches csharp-publisher/Program.cs s_testSet
    private static readonly (string Label, byte[] Payload)[] s_gsfapiPayloads =
    [
        ("ASCII",  "HELLO BUFFERBLOCK"u8.ToArray()),
        ("BINARY", Enumerable.Range(0, 256).Select(i => (byte)i).ToArray()),
        ("JSON",   System.Text.Encoding.UTF8.GetBytes("""{"signalid":"aabbccdd-1122-3344-5566-778899aabbcc","value":1.0,"type":"test-event"}""")),
    ];

    private static (string Label, byte[] Payload)[] s_expectedPayloads = s_pyapiPayloads;

    private static int s_received;
    private static int s_mismatches;

    private static void Status(string message)
    {
        lock (s_outLock)
            Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
    }

    private static int Main(string[] args)
    {
        string host = "localhost";
        ushort port = 7195;
        int expectSets = 0;
        double timeoutSeconds = 60.0;

        foreach (string arg in args)
        {
            if (arg.StartsWith("--host=", StringComparison.OrdinalIgnoreCase))
                host = arg["--host=".Length..];
            else if (arg.StartsWith("--port=", StringComparison.OrdinalIgnoreCase))
                port = ushort.Parse(arg["--port=".Length..]);
            else if (arg.StartsWith("--expect-sets=", StringComparison.OrdinalIgnoreCase))
                expectSets = int.Parse(arg["--expect-sets=".Length..]);
            else if (arg.StartsWith("--timeout=", StringComparison.OrdinalIgnoreCase))
                timeoutSeconds = double.Parse(arg["--timeout=".Length..]);
            else if (arg.StartsWith("--publisher=", StringComparison.OrdinalIgnoreCase))
            {
                string pub = arg["--publisher=".Length..].ToLowerInvariant();
                s_expectedPayloads = pub switch
                {
                    "csharp" or "gsfapi" => s_gsfapiPayloads,
                    "python" or "pyapi"  => s_pyapiPayloads,
                    _ => throw new ArgumentException($"Unknown --publisher value '{pub}'; expected csharp|gsfapi|python|pyapi")
                };
            }
            else if (arg.StartsWith("--help", StringComparison.OrdinalIgnoreCase))
            {
                PrintUsage();
                return 0;
            }
        }

        Status("=== STTP BufferBlock Interop Test Subscriber (gsfapi) ===");
        Status($"Publisher:  {host}:{port}");
        if (expectSets > 0)
            Status($"Expecting {expectSets} test sets x {s_expectedPayloads.Length} blocks = {expectSets * s_expectedPayloads.Length} buffer blocks total");
        Status($"Timeout:    {timeoutSeconds:F1}s");
        Status("");

        InitializeGemstoneSettings();

        // Subscriber uses ProcessException for errors; status messages flow through StatusMessage.
        DataSubscriber subscriber = new();

        ManualResetEventSlim completionEvent = new(false);

        subscriber.StatusMessage += (_, e) => Status($"[SUB] {e.Argument}");
        subscriber.ProcessException += (_, e) => Status($"[SUB][ERR] {e.Argument.Message}");

        subscriber.ConnectionEstablished += (_, _) =>
        {
            Status("[SUB] Connection established - issuing subscribe...");
            subscriber.Subscribe(new SubscriptionInfo
            {
                FilterExpression = "FILTER ActiveMeasurements WHERE True",
            });
        };

        subscriber.ConnectionTerminated += (_, _) =>
        {
            Status("[SUB] Connection terminated");
        };

        subscriber.NewMeasurements += (_, e) =>
        {
            foreach (IMeasurement measurement in e.Argument)
            {
                if (measurement is not BufferBlockMeasurement { Buffer: not null } bb)
                    continue;

                int idx = s_received % s_expectedPayloads.Length;
                s_received++;
                (string label, byte[] expected) = s_expectedPayloads[idx];

                ReadOnlySpan<byte> actualSpan = bb.Buffer.AsSpan(0, bb.Length);
                bool ok = actualSpan.SequenceEqual(expected);
                if (!ok)
                    s_mismatches++;

                BufferBlockFlags flags = (BufferBlockFlags)bb.Flags;
                bool reqConfirm = flags.HasFlag(BufferBlockFlags.RequireConfirmation);
                bool compressed = flags.HasFlag(BufferBlockFlags.Compressed);
                bool keyIndex = flags.HasFlag(BufferBlockFlags.KeyIndex);
                string marker = ok ? "OK " : "MISMATCH";
                Status($"  << RECEIVED #{s_received,4:D} [{label,-6}] {marker} signalid={bb.Key.SignalID:D} bytes={bb.Length} expected={expected.Length} flags=0x{bb.Flags:x2} (req_confirm={reqConfirm}, compressed={compressed}, key_index={keyIndex})");

                if (!ok)
                {
                    Status($"     expected: {Hex(expected)}");
                    Status($"     actual  : {Hex(bb.Buffer.AsSpan(0, bb.Length).ToArray())}");
                }

                if (expectSets > 0 && s_received >= expectSets * s_expectedPayloads.Length)
                    completionEvent.Set();
            }
        };

        subscriber.ConnectionString = $"server={host}:{port}";

        // TSSC has nothing to do with buffer blocks - they go on the non-TSSC path - but leaving
        // compression enabled here verifies the subscriber correctly ignores it for buffer blocks.
        subscriber.CompressionModes = CompressionModes.TSSC | CompressionModes.GZip;
        subscriber.Initialize();
        subscriber.Start();

        bool completed = completionEvent.Wait(TimeSpan.FromSeconds(timeoutSeconds));

        subscriber.Stop();
        subscriber.Dispose();

        Status("");
        Status("=== Summary ===");
        Status($"Buffer blocks received: {s_received}");
        Status($"Mismatches:             {s_mismatches}");

        if (s_mismatches > 0)
            return 1;

        if (expectSets > 0 && !completed)
        {
            Status($"[TIMEOUT] Only received {s_received} buffer blocks; expected {expectSets * s_expectedPayloads.Length}");
            return 2;
        }

        return 0;
    }

    private static string Hex(byte[] buf, int max = 32)
    {
        int take = Math.Min(buf.Length, max);
        string head = string.Join(' ', buf.Take(take).Select(b => $"{b:x2}"));
        return buf.Length > max ? $"{head}..." : head;
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
        Console.WriteLine("Usage: SttpBufferBlockInteropSubscriber [--host=NAME] [--port=NNNN] [--expect-sets=N] [--timeout=N]");
        Console.WriteLine();
        Console.WriteLine("  --host=NAME       Publisher hostname (default localhost)");
        Console.WriteLine("  --port=NNNN       Publisher port (default 7195)");
        Console.WriteLine("  --expect-sets=N   Exit successfully after N complete test sets (3 blocks each)");
        Console.WriteLine("  --timeout=N       Maximum seconds to wait (default 60)");
    }
}
