# STTP gsfapi ↔ pyapi Single-Measurement Interop Test

A focused two-process test harness for investigating the user's reported issue
that a **single measurement** published by the C# `gsfapi` (Gemstone .NET 9.0)
STTP publisher was not always received by a Python `pyapi` STTP subscriber in
the **WaveAppsSample** environment.

## Components

```
test-interop/
├── Directory.Build.props  forces Configuration=Development for IDE builds
├── csharp-publisher/      .NET 9.0 console, ProjectReference into ..\..\gsfapi
│   ├── csharp-publisher.csproj
│   ├── Program.cs
│   ├── build.cmd          wrapper: `dotnet build -c Development`
│   └── run.cmd            wrapper: `dotnet run -c Development -- ...`
└── python-subscriber/     pyapi-based subscriber, runs against C:\Projects\sttp\pyapi\src
    └── main.py
```

The C# publisher is a *minimal* DataPublisher host:

* **Project reference, not NuGet.** The csproj has
  `<ProjectReference Include="..\..\gsfapi\src\lib\sttp.gemstone\sttp.gemstone.csproj" />`.
  This means the publisher links the live gsfapi sources, so a debugger can step
  straight into `DataPublisher`, `SubscriberAdapter`, `TsscEncoder`, etc., and
  any local edits to those files take effect on the next build with no NuGet
  publish step. Inspired by
  [`C:\Projects\gpa\gsf\Source\Applications\DataPublisherTest`](C:\Projects\gpa\gsf\Source\Applications\DataPublisherTest)
  (the GEP equivalent test app).
* **Configuration=Development is required.** The `sttp.gemstone` csproj has
  `Debug`/`Release` (PackageReference to Gemstone.* 1.0.172, often stale vs.
  the live `sttp.core` source) and `Development` (ProjectReference to local
  `C:\Projects\gemstone\*` source). Building with the default `Debug` fails:
  `error CS1739: AdapterProtocolAttribute does not have a parameter named
  'lockedDeviceFields'` (the attribute moved forward; the package is behind).
  Always use `-c Development` — the `build.cmd` / `run.cmd` wrappers do this
  for you, and `Directory.Build.props` sets it as the default for IDE builds.
* Subclasses `DataPublisher` to override `AcquireMetadata` so it can serve
  metadata from an in-memory `DataSet` rather than from an ADO database.
  (The default `AcquireMetadata` opens an `AdoDataConnection` from
  `ConfigSettings.Default` — we don't have one in this standalone test, so
  the override is required.)
* Initializes Gemstone configuration (`InitializeGemstoneSettings`) so static
  initializers in `Gemstone.Diagnostics.Logger` and
  `Gemstone.Timeseries.Statistics.StatisticsEngine` succeed.
* Builds a single-row `ActiveMeasurements` / `MeasurementDetail` /
  `DeviceDetail` metadata DataSet for one fixed signal ID.

## Running

### C# publisher

Always build/run with `Configuration=Development` (the wrappers do this for you):

```powershell
cd C:\Projects\sttp\test-interop\csharp-publisher

# Build (project ref into ..\..\gsfapi)
.\build.cmd

# TSSC enabled (default), interactive mode - press P to send 1 measurement
.\run.cmd

# Same, but auto-publish 1 measurement every 2 seconds while a subscriber is connected
.\run.cmd --auto

# Send EXACTLY ONE measurement 5s after the subscriber connects, then idle
.\run.cmd --once

# Same, but with TSSC disabled (CompactMeasurement on the wire instead of TSSC)
.\run.cmd --once --no-tssc

# Custom port and 10s pre-publish delay
.\run.cmd --port=7170 --once --once-delay=10

# Equivalent without the wrappers:
dotnet build -c Development
dotnet run -c Development --no-build -- --once
```

**Debugging tip.** Because the publisher uses a `ProjectReference` to
`..\..\gsfapi\src\lib\sttp.gemstone`, you can set breakpoints anywhere in the
gsfapi source (e.g., [`SubscriberAdapter.ProcessTSSCMeasurements`](../gsfapi/src/lib/sttp.core/SubscriberAdapter.cs#L805))
and step in directly. Open the publisher project in your IDE with
Configuration=Development and the gsfapi PDB symbols are loaded automatically.

### Python subscriber

```powershell
cd C:\Projects\sttp\test-interop\python-subscriber

# TSSC enabled (default), Enter to exit
python main.py

# 14 seconds then auto-exit
python main.py --timeout 14

# TSSC disabled
python main.py --no-tssc

# Custom host/port
python main.py --host localhost --port 7170 --timeout 30
```

The subscriber prints every received measurement on its own line:
`<< RECEIVED #N: signalid=… ts=… value=… flags=…`.

## Findings

### 1) TSSC=on, single delayed measurement → ✅ delivered

Test: `--once --once-delay=5` (publisher), `--timeout 20` (subscriber).
The publisher waits for the subscriber, then sleeps 5 s and sends exactly
one measurement.

Result: the single measurement arrived correctly. The publisher logs
`>> PUBLISHING measurement #1` followed by `Start time sent` and
`TSSC algorithm reset before sequence number: 0`; the subscriber logs
`<< RECEIVED #1` within ~10 ms.

**Conclusion:** the `gsfapi` TSSC encoder *does* flush and transmit a single
measurement, including for the very first packet after subscribe. The hypothesis
that "TSSC buffers a single measurement until more arrive" is **not reproducible**
in this minimal harness.

### 2) TSSC=on, periodic single measurements (every 2 s) → ✅ all delivered

Test: `--auto` (publisher), `--timeout 14` (subscriber). Publisher sends one
measurement every 2 s.

Result: 7/7 measurements published, 7/7 received.

### 3) TSSC=off → ⚠️ pyapi bug uncovered (independent of the original issue)

Test: `--no-tssc` on both sides.

Result: subscriber crashes on the first uncompressed CompactMeasurement with:

```
[SUB][ERR] Exception processing server response:
  np.int8(0) is not a valid CompactStateFlags
```

The bug is in [`pyapi/src/sttp/transport/compactmeasurement.py:211`](../pyapi/src/sttp/transport/compactmeasurement.py#L211):

```python
flags = CompactStateFlags(value)   # value is np.byte(0) for StateFlags.Normal
```

Under Python 3.12's stricter `IntFlag.__new__`, calling
`CompactStateFlags(np.int8(0))` raises `ValueError` because numpy's signed-int
zero is rejected by `enum._missing_`. The subscriber then drops the connection
and infinite-reconnects.

**Reproducer:**

```python
from enum import IntFlag
import numpy as np

class CompactStateFlags(IntFlag):
    DATARANGE       = 0x01
    DATAQUALITY     = 0x02
    TIMEQUALITY     = 0x04
    SYSTEMISSUE     = 0x08
    CALCULATEDVALUE = 0x10
    ALARMVALUE      = 0x20
    BASETIMEOFFSET  = 0x40
    TIMEINDEX       = 0x80

CompactStateFlags(np.byte(0))    # ValueError: np.int8(0) is not a valid CompactStateFlags
CompactStateFlags(int(0))        # OK
```

**Suggested fix in `compactmeasurement.py`:** coerce to a built-in `int` before
constructing the enum, e.g.

```python
flags = CompactStateFlags(int(value))
```

…or use `IntFlag` with `boundary=KEEP` (Python 3.11+).

This is a real interop bug for `gsfapi → pyapi` whenever TSSC is disabled and
a measurement carries `StateFlags.Normal` (0). It explains why the user's
"sample event measurements" (which often have `StateFlags = Normal`) might be
dropped on the pyapi side when something causes TSSC to be off — but only if
TSSC is off; with TSSC on, the value is folded into the bitstream and never
hits this code path.

### 4) Why the original issue did not reproduce here

The user's environment is the **WaveAppsSample**, where the gsfapi publisher
runs as a `FacileActionAdapter`/host adapter inside an IAOSession with the
GSF time-series host. Several mechanisms in that environment can buffer or
drop single measurements *before* they reach the TSSC encoder:

* `SubscriberAdapter.QueueMeasurementsForProcessing` has two non-default
  paths that *do* buffer single measurements:
  * `TrackLatestMeasurements` (set when the subscriber requests
    `Throttled=true`): only flushes every `LagTime` seconds (default 5 s).
  * `TimeSortedPublication`: routes through a `Concentrator` with `LagTime`
    seconds of waiting before any frame leaves.

  See [`SubscriberAdapter.cs:514-552`](../gsfapi/src/lib/sttp.core/SubscriberAdapter.cs#L514).
* `m_routingTables.InjectMeasurements(...)` in `DataPublisher.QueueMeasurementsForProcessing`
  ([`DataPublisher.cs:1773-1798`](../gsfapi/src/lib/sttp.core/DataPublisher.cs#L1773))
  routes through GSF routing tables - these can also buffer.
* The default route mode is `RoutingMethod.HighLatencyLowCpu` *or* the standard
  routing tables; both are tunable via `OptimizationOptions.DefaultRoutingMethod`.

In a host-adapter setup, the user's measurement is queued via
`OnNewMeasurements` from another adapter. Depending on routing-table tuning and
how often the host pumps the routing engine, a *single* measurement may be
indistinguishable from "no measurements" for buffering purposes and may sit in
a routing/concentrator queue until the next batch arrives.

In our standalone harness we call
`publisher.QueueMeasurementsForProcessing(new[] { measurement })` directly
on a publisher that has no concentrator, no `Throttled` flag, and no time-sorted
publication, so the measurement reaches the TSSC encoder immediately.

## Where to look next in the WaveAppsSample / gsfapi side

When investigating the dropped-single-measurement scenario in the *real* host:

1. Inspect the live publisher's connection settings and check whether the
   subscribing client passed `Throttled=true` or whether
   `TimeSortedPublication=true` is set on the publisher's adapter settings -
   either causes single measurements to wait up to `LagTime`s.
2. Check the `Concentrator.Status` (printed via `m_proxyDataPublisher.Status`)
   to see if there's a non-zero queued measurement count between sends.
3. Add a `[PUB] Starting measurement route calculation...` watcher and look at
   the latency between `OnNewMeasurements` and the publisher's
   `ProcessTSSCMeasurements` invocation - if the routing tables are
   batching, single measurements can sit in the routing queue.
4. Confirm `connection.OperationalModes.HasFlag(OperationalModes.CompressPayloadData)`
   and `AllowPayloadCompression == true` on the publisher; without both, the
   wire format is `CompactMeasurement` and the pyapi bug in finding (3)
   applies.

## Investigation pointers (file:line)

* Publisher TSSC encoder flow:
  [`gsfapi/src/lib/sttp.core/SubscriberAdapter.cs:805`](../gsfapi/src/lib/sttp.core/SubscriberAdapter.cs#L805)
  (`ProcessTSSCMeasurements`) — the foreach loop ends with
  `if (count > 0) SendTSSCPayload(count, currentCacheIndex)`, so a single
  measurement *should* always be flushed.
* Publisher pre-encoder buffering:
  [`gsfapi/src/lib/sttp.core/SubscriberAdapter.cs:514-552`](../gsfapi/src/lib/sttp.core/SubscriberAdapter.cs#L514)
  (`QueueMeasurementsForProcessing`) — two paths that buffer single measurements.
* Subscriber TSSC decoder:
  [`pyapi/src/sttp/transport/datasubscriber.py:978`](../pyapi/src/sttp/transport/datasubscriber.py#L978)
  (`_parse_tssc_measurements`) — silently drops the packet on
  `decoder.sequencenumber != sequencenumber` mismatch.
* Subscriber CompactMeasurement decoder bug:
  [`pyapi/src/sttp/transport/compactmeasurement.py:205-218`](../pyapi/src/sttp/transport/compactmeasurement.py#L205)
  (`set_compact_stateflags`) — fails on `StateFlags.Normal` (0) under Python 3.12.
* Default metadata SQL (publisher requires DB unless overridden):
  [`gsfapi/src/lib/sttp.core/DataPublisher.cs:757`](../gsfapi/src/lib/sttp.core/DataPublisher.cs#L757)
  (`DefaultMetadataTables`).
