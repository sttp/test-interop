# ******************************************************************************************************
#  main.py - STTP Interop BufferBlock Test Subscriber
#
#  Receives `ServerResponse.BufferBlock` frames from the C# gsfapi publisher in
#  test-interop/bufferblock/csharp-publisher and verifies each payload matches the deterministic
#  test set byte-for-byte.
#
#  The C# publisher sends three buffer blocks per test set:
#      1. ASCII text       "HELLO BUFFERBLOCK"
#      2. Binary counter   bytes 0x00..0xFF
#      3. JSON event       a UTF-8 JSON document
#  Keep this list in sync with `Program.cs::s_testSet` on the publisher side.
# ******************************************************************************************************

import argparse
import os
import sys
from datetime import datetime
from threading import Event
from typing import List

# Use the local sttp source tree at C:\Projects\sttp\pyapi\src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'pyapi', 'src')))

from gsf import Limits  # noqa: E402
from sttp.config import Config  # noqa: E402
from sttp.subscriber import Subscriber  # noqa: E402
from sttp.settings import Settings  # noqa: E402
from sttp.transport.bufferblock import BufferBlock  # noqa: E402
from sttp.transport.constants import BufferBlockFlags  # noqa: E402

MAXPORT = Limits.MAXUINT16

# Expected payloads - must mirror C# Program.cs::s_testSet exactly.
EXPECTED_PAYLOADS: List[tuple[str, bytes]] = [
    ("ASCII",  b"HELLO BUFFERBLOCK"),
    ("BINARY", bytes(range(256))),
    ("JSON",   b'{"signalid":"aabbccdd-1122-3344-5566-778899aabbcc","value":1.0,"type":"test-event"}'),
]


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"


def status(message: str) -> None:
    print(f"[{stamp()}] {message}", flush=True)


def hexdump(buf: bytes, max_bytes: int = 32) -> str:
    """Compact hex preview for diagnostic logs."""
    head = buf[:max_bytes]
    tail = "..." if len(buf) > max_bytes else ""
    return " ".join(f"{b:02x}" for b in head) + tail


def main() -> int:
    parser = argparse.ArgumentParser(description="STTP buffer-block interop test subscriber")
    parser.add_argument("--host", default="localhost", help="Publisher hostname (default localhost)")
    parser.add_argument("--port", type=int, default=7175, help="Publisher port (default 7175)")
    parser.add_argument(
        "--filter",
        default="FILTER ActiveMeasurements WHERE True",
        help="Subscription filter expression",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Auto-exit after this many seconds (0 = wait for Enter, only valid in TTY)",
    )
    parser.add_argument(
        "--expect-sets",
        type=int,
        default=0,
        help="Exit with success once this many complete test sets (3 blocks each) have been received",
    )
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number {args.port} is out of range: must be 1 to {MAXPORT}")
        return 2

    status("=== STTP BufferBlock Interop Test Subscriber ===")
    status(f"Publisher:           {args.host}:{args.port}")
    status(f"Subscription filter: {args.filter}")
    if args.expect_sets:
        status(f"Will exit successfully after {args.expect_sets} complete test sets ({args.expect_sets * 3} blocks).")
    status("")

    config = Config()
    config.compress_payloaddata = True
    config.compress_metadata = True
    config.compress_signalindexcache = True
    config.autoreconnect = True

    settings = Settings()
    settings.usemillisecondresolution = False
    settings.includetime = True

    subscriber = Subscriber()

    subscriber.statusmessage_logger = lambda msg: status(f"[SUB] {msg}")
    subscriber.errormessage_logger = lambda msg: status(f"[SUB][ERR] {msg}")

    received_count = [0]
    mismatch_count = [0]
    completion_event = Event()

    def on_new_buffer_blocks(buffer_blocks: List[BufferBlock]) -> None:
        for bb in buffer_blocks:
            received_count[0] += 1
            idx = (received_count[0] - 1) % len(EXPECTED_PAYLOADS)
            label, expected = EXPECTED_PAYLOADS[idx]
            actual = bytes(bb.buffer) if bb.buffer is not None else b""

            ok = actual == expected
            if not ok:
                mismatch_count[0] += 1

            require_conf = BufferBlockFlags.REQUIRECONFIRMATION in bb.flags
            compressed = BufferBlockFlags.COMPRESSED in bb.flags
            key_index = BufferBlockFlags.KEYINDEX in bb.flags
            marker = "OK " if ok else "MISMATCH"
            status(
                f"  << RECEIVED #{received_count[0]:>4d} [{label:<6s}] {marker} "
                f"signalid={bb.signalid} bytes={len(actual)} expected={len(expected)} "
                f"flags=0x{int(bb.flags):02x} (req_confirm={require_conf}, compressed={compressed}, key_index={key_index})"
            )

            if not ok:
                status(f"     expected: {hexdump(expected)}")
                status(f"     actual  : {hexdump(actual)}")

            if args.expect_sets and received_count[0] >= args.expect_sets * len(EXPECTED_PAYLOADS):
                completion_event.set()

    def on_connection_established() -> None:
        status("[SUB] Connection established - waiting for buffer blocks...")

    def on_connection_terminated() -> None:
        status("[SUB] Connection terminated")

    def on_subscription_updated(cache) -> None:
        count = cache.count if hasattr(cache, 'count') else 0
        status(f"[SUB] Subscription updated: signal index cache has {count} mappings")

    subscriber.connectionestablished_receiver = on_connection_established
    subscriber.connectionterminated_receiver = on_connection_terminated
    subscriber.newbufferblock_receiver = on_new_buffer_blocks
    subscriber.subscriptionupdated_receiver = on_subscription_updated

    try:
        subscriber.subscribe(args.filter, settings)
        subscriber.connect(f"{args.host}:{args.port}", config)

        if args.expect_sets:
            timed_out = not completion_event.wait(timeout=args.timeout if args.timeout > 0 else 60.0)
            if timed_out:
                status(f"[SUB][TIMEOUT] Only received {received_count[0]} buffer blocks; expected {args.expect_sets * len(EXPECTED_PAYLOADS)}")
        elif args.timeout > 0:
            status(f"Will auto-exit after {args.timeout:.1f} seconds.")
            from time import sleep
            sleep(args.timeout)
        elif sys.stdin.isatty():
            status("Press Enter to disconnect and exit.")
            input()
        else:
            status("stdin redirected and no --timeout - sleeping forever (Ctrl+C to stop).")
            try:
                from time import sleep
                while True:
                    sleep(60)
            except KeyboardInterrupt:
                pass
    finally:
        subscriber.dispose()
        status("")
        status(f"=== Summary ===")
        status(f"Buffer blocks received: {received_count[0]}")
        status(f"Mismatches:             {mismatch_count[0]}")

    if mismatch_count[0] > 0:
        return 1
    if args.expect_sets and received_count[0] < args.expect_sets * len(EXPECTED_PAYLOADS):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
