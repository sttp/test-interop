# STTP BufferBlock Interop Harness

Bidirectional, byte-exact validation of the IEEE 2664-2024 § 5.5.10 BUFFER BLOCK wire
format across gsfapi (C#) and pyapi (Python). Each test set is three deterministic
buffer blocks; the receiver hexdumps any mismatch.

| Direction | Sender | Receiver | What it validates |
|---|---|---|---|
| **gsfapi → pyapi** | [csharp-publisher/](csharp-publisher) | [python-subscriber/](python-subscriber) | pyapi `_handle_bufferblock` parses the IEEE flags byte and GZip-decompresses |
| **pyapi → gsfapi** | [python-publisher/](python-publisher) | [csharp-subscriber/](csharp-subscriber) | pyapi `SubscriberConnection.send_buffer_block` emits an IEEE-aligned frame |
| **gsfapi → gsfapi** | [csharp-publisher/](csharp-publisher) | [csharp-subscriber/](csharp-subscriber) | gsfapi round-trip (`--publisher=csharp`) after the over-allocation + TSSC fixes |

## Wire format (post-fix)

Per IEEE Std 2664-2024 § 5.5.10 Figure 34, with the gsfapi `SIGNAL INDEX` extension at +5
(the IEEE figure has no in-band signal identifier; including the runtime ID lets one
subscription carry buffer blocks for multiple signals):

```
+0  uint32  SEQUENCE VALUE      (big-endian, ack tracker)
+4  byte    BUFFER BLOCK FLAGS  (Table 8: 0x01 RequireConfirmation, 0x08 Compressed, 0x10 CacheIndex)
+5  int32   SIGNAL INDEX        (big-endian, runtime ID)
+9  byte[]  PAYLOAD             (GZip-compressed when COMPRESSED flag set)
```

GZip is the IEEE-mandated default for buffer-block payload compression (Annex /
`BufferBlockPayloadCompressionAlgorithms`, default `Gzip, Gzip`).

## KEY INDEX flag (UDP encryption)

Per IEEE 2664-2024 Table 8 bit `0x04`, BufferBlock payloads can be AES-encrypted over UDP using
the cipher key sets negotiated by `ROTATE CIPHER KEYS` / `UPDATE CIPHER KEYS`. The wire layout
preserves `SEQUENCE VALUE` and `FLAGS` cleartext (so the receiver can dedup retransmits and
choose the cipher key set before decryption); `SIGNAL INDEX` and `PAYLOAD` are encrypted
together as a single AES-CBC block, mirroring the DATA PACKET pattern where
`MEASUREMENT COUNT` + `PAYLOAD` are encrypted while only `FLAGS` stays cleartext.

When both COMPRESSED and KEY INDEX apply, compression happens first per IEEE Std 2664-2024
§ 5.5.10 ("If both UDP compression and encryption is enabled, data is compressed first and
then encrypted") - so the receiver decrypts before decompressing.

Implementation status:
- **gsfapi publisher**: encrypts BufferBlock payloads in `SendClientResponse`, sets
  `KEY INDEX` from `connection.CipherIndex` when `KeyIVs` is non-null. Parallel to the
  existing `CipherIndex` handling for DATA PACKET.
- **gsfapi subscriber**: parses `KEY INDEX` from the flags byte, AES-decrypts
  `SIGNAL INDEX + PAYLOAD` using the matching key set before reading signal index.
- **pyapi subscriber**: same as gsfapi - parses the bit and decrypts. Reuses the existing
  AES-CBC + `_key_ivs` infrastructure that already handles DATA PACKET decryption.
- **pyapi publisher**: not applicable - pyapi has no UDP send path for BufferBlocks (sends
  over the TCP command channel only, where TLS already covers confidentiality), so the
  `KEY INDEX` bit is never set on send. Receive parity preserves byte-exact interop with a
  gsfapi peer that does emit encrypted BufferBlocks.

Receive-side validation: subscribers print `key_index=True/False` per block alongside
`req_confirm` and `compressed`. The default test harness does not negotiate UDP cipher keys,
so all observed flags read `key_index=False`; the bit is exercised end-to-end whenever a
real session establishes UDP cipher keys via `ROTATE CIPHER KEYS`.

## REQUIRE CONFIRMATION flag

Per IEEE 2664-2024 Table 8 bit `0x01`, each buffer block can opt in or out of receiver
acknowledgement. Both publishers expose `--no-confirm` to clear the bit; receivers report
the parsed flag state inline (`flags=0x09` = `REQUIRE CONFIRMATION | COMPRESSED`,
`flags=0x08` = `COMPRESSED` only).

When the bit is **set** (the default):
- Publisher caches each block in the retransmission cache and arms the retransmission timer
- Subscriber emits `CONFIRM BUFFER BLOCK` (`ServerCommand 0x08`) on receipt
- Publisher trims the cache as acks arrive; retransmits unacked blocks on timer expiry

When the bit is **clear**:
- Publisher fire-and-forget — no cache entry, timer stays stopped
- Subscriber skips the ack but still advances sequence-number bookkeeping (ordering / dropout
  detection still works for a peer that knows to look)

`pyapi.transport.bufferblock.BufferBlock.flags` and
`Gemstone.Timeseries.BufferBlockMeasurement.Flags` expose the parsed flag byte on the
receive side; `Gemstone.Timeseries.BufferBlockMeasurement.RequireConfirmation` is the
parallel publisher-side knob (defaults `true`). pyapi's send methods take a
`require_confirmation: bool = True` keyword.

## Bugs fixed during this harness's development

Three real issues turned up while building it, all fixed before declaring the harness
green:

1. **gsfapi over-allocated the on-wire buffer by 6 bytes** —
   `SubscriberAdapter.cs::ProcessMeasurements` declared
   `byte[] bufferBlock = new byte[BufferBlockHeaderSize + 4 + bufferBlockMeasurement.Length]`
   (15 + L) but only wrote 9 + L bytes of content. The trailing 6 zero-init bytes leaked
   onto the wire and gsfapi's own subscriber consumed them
   (`new BufferBlockMeasurement(buffer, +9, responseLength - 9)` is 6 too long). Fixed
   by allocating exactly `9 + Length`.
2. **gsfapi's TSSC path silently dropped buffer blocks** —
   `ProcessTSSCMeasurements` had no BufferBlockMeasurement branch and fed each one
   through the TSSC encoder as if it were a regular measurement (using `AdjustedValue`,
   which is NaN). Since TSSC is essentially always on in production, this made buffer
   blocks unusable in real deployments. Fixed by detecting BufferBlockMeasurement at the
   top of the TSSC loop, flushing any pending TSSC payload, and sending the buffer block
   on its own frame with GZip-compressed payload (`BufferBlockFlags.Compressed`).
3. **gsfapi wrote `cacheIndex` as a raw int (0 or 1) at offset +4** —
   not matching IEEE Table 8 where CACHE INDEX is bit `0x10` of a flags byte. Fixed by
   introducing a `BufferBlockFlags` enum (parallel to `DataPacketFlags`) and writing
   `CacheIndex` as bit `0x10`, with `Compressed` at `0x08` and `RequireConfirmation` at
   `0x01`. pyapi matches.

## Running

Build prerequisites: the gsfapi sources at `..\..\gsfapi` and a Python 3.10+ interpreter
with numpy. The harness uses ProjectReferences into local Gemstone source, so a build
out of `Configuration=Development` is required — the `build.cmd` / `run.cmd` wrappers
set this for you.

### gsfapi → pyapi (TSSC on, GZip-compressed payloads)

```powershell
# Terminal 1
cd C:\Projects\sttp\test-interop\bufferblock\csharp-publisher
.\build.cmd
.\run.cmd --port=7201 --auto --auto-count=2 --auto-delay=3

# Terminal 2 (or wait ~3s after publisher logs "waiting for subscriber to connect...")
cd C:\Projects\sttp\test-interop\bufferblock\python-subscriber
python main.py --host localhost --port 7201 --expect-sets=2 --timeout=45
```

To force the non-TSSC publisher path, add `--no-tssc` on the C# side.

### pyapi → gsfapi

```powershell
# Terminal 1
cd C:\Projects\sttp\test-interop\bufferblock\python-publisher
python main.py --port=7202 --auto-count=2 --auto-delay=3

# Terminal 2
cd C:\Projects\sttp\test-interop\bufferblock\csharp-subscriber
.\build.cmd
.\run.cmd --host=localhost --port=7202 --publisher=python --expect-sets=2 --timeout=45
```

The pyapi publisher's `send_buffer_block` automatically sets the COMPRESSED flag and
GZip-compresses when the session has payload compression negotiated.

### gsfapi → gsfapi (round-trip)

```powershell
# Terminal 1
cd C:\Projects\sttp\test-interop\bufferblock\csharp-publisher
.\run.cmd --port=7197 --auto --auto-count=2 --auto-delay=3   # TSSC off path
# or with TSSC on (default):
.\run.cmd --port=7198 --auto --auto-count=2 --auto-delay=3

# Terminal 2
cd C:\Projects\sttp\test-interop\bufferblock\csharp-subscriber
.\run.cmd --host=localhost --port=7197 --publisher=csharp --expect-sets=2 --timeout=45
```

All three variants exit with `Mismatches: 0` and status code 0.

## Results

| Variant | Receiver report |
|---|---|
| gsfapi → gsfapi (TSSC off) | Buffer blocks received: 6, Mismatches: 0 |
| gsfapi → gsfapi (TSSC on)  | Buffer blocks received: 6, Mismatches: 0 |
| gsfapi → pyapi  (TSSC on)  | Buffer blocks received: 6, Mismatches: 0 |
| pyapi  → gsfapi            | Buffer blocks received: 6, Mismatches: 0 |
