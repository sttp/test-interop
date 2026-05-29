# STTP Interoperability Test Suite

This repository contains interoperability test harnesses for validating the IEEE 2664-2024 (STTP) protocol implementation across different language implementations: **gsfapi** (C# / .NET) and **pyapi** (Python).

## Purpose

The test-interop project ensures wire-format compatibility and behavioral fidelity between STTP publisher and subscriber implementations. Each test validates specific aspects of the protocol, including:

- **Wire format compliance** – byte-exact validation of IEEE 2664-2024 § 5.5.10 buffer blocks
- **Cross-language interoperability** – bidirectional C# ↔ Python data exchange
- **Compression handling** – GZip payload compression/decompression
- **Encryption support** – AES-CBC encryption with KEY INDEX flags (UDP)
- **Metadata exchange** – measurement metadata serialization and parsing
- **Single-measurement transmission** – focused validation of individual data points

## Test Projects

### [bufferblock-test](bufferblock-test)

Bidirectional, byte-exact validation of the IEEE 2664-2024 § 5.5.10 BUFFER BLOCK wire format across gsfapi (C#) and pyapi (Python).

**Test Scenarios:**
- **C# → Python** – [`csharp-publisher`](bufferblock-test/csharp-publisher) → [`python-subscriber`](bufferblock-test/python-subscriber)
- **Python → C#** – [`python-publisher`](bufferblock-test/python-publisher) → [`csharp-subscriber`](bufferblock-test/csharp-subscriber)
- **C# → C#** – [`csharp-publisher`](bufferblock-test/csharp-publisher) → [`csharp-subscriber`](bufferblock-test/csharp-subscriber)

Each test sends three deterministic buffer blocks; mismatches are hexdumped for analysis.

### [one-measurement-test](one-measurement-test)

A focused two-process test harness for investigating single-measurement transmission between C# publishers and Python subscribers.

**Components:**
- [`csharp-publisher`](one-measurement-test/csharp-publisher) – Minimal .NET 9.0 DataPublisher with in-memory metadata
- [`python-subscriber`](one-measurement-test/python-subscriber) – pyapi-based subscriber for validation

This test uses project references (not NuGet) to link live gsfapi sources, enabling direct debugging into DataPublisher, SubscriberAdapter, and TSSCEncoder.

## Build Requirements

- **.NET 9.0 SDK** (for C# projects)
- **Python 3.8+** (for Python projects)
- **Configuration=Development** required for C# projects (enforced by [`Directory.Build.props`](Directory.Build.props))

Each test folder contains its own README with detailed build and execution instructions.

## Related Projects

- **gsfapi** – Grid Solutions Framework / Gemstone (C#/.NET-based) implementation of IEEE 2664-2024 STTP
- **pyapi** – Python implementation of IEEE 2664-2024 STTP

## IEEE 2664-2024 Reference

The Streaming Telemetry Transport Protocol (STTP) is defined in IEEE Std 2664-2024, specifying wire formats, compression algorithms, encryption schemes, and metadata exchange patterns for high-throughput time-series data streaming.
