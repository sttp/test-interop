# ******************************************************************************************************
#  main.py - STTP Interop BufferBlock Test Publisher (pyapi side)
#
#  Sends buffer blocks with deterministic payloads to validate the pyapi publisher path
#  (`SubscriberConnection.send_buffer_block` and `Publisher.broadcast_buffer_block`). The C#
#  subscriber on the other side verifies each payload byte-for-byte.
#
#  Test set (must mirror csharp-subscriber/Program.cs s_expectedPayloads):
#      1. ASCII text   "HELLO FROM PY"
#      2. Binary       bytes 0xFF..0x00 (reverse counter)
#      3. JSON event   a UTF-8 JSON document
# ******************************************************************************************************

import argparse
import os
import sys
import time
from datetime import datetime
from threading import Event
from typing import List, Tuple

# Use the local sttp source tree at C:\Projects\sttp\pyapi\src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'pyapi', 'src')))

from gsf import Limits  # noqa: E402
from sttp.publisher import Publisher  # noqa: E402
from sttp.transport.subscriberconnection import SubscriberConnection  # noqa: E402
from uuid import UUID  # noqa: E402

MAXPORT = Limits.MAXUINT16

# Signal ID must match Metadata.xml exactly.
SIGNAL_ID = UUID("deadbeef-1111-2222-3333-444455556666")

# Test set - keep in lock-step with csharp-subscriber/Program.cs s_expectedPayloads.
TEST_SET: List[Tuple[str, bytes]] = [
    ("ASCII",  b"HELLO FROM PY"),
    ("BINARY", bytes(reversed(range(256)))),
    ("JSON",   b'{"source":"pyapi","value":42.0,"type":"buffer-block-test"}'),
]


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"


def status(message: str) -> None:
    print(f"[{stamp()}] {message}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="STTP buffer-block interop test publisher (pyapi)")
    parser.add_argument("--port", type=int, default=7195, help="TCP port to listen on (default 7195)")
    parser.add_argument(
        "--auto-count",
        type=int,
        default=2,
        help="Number of times to send the 3-buffer-block test set (default 2)",
    )
    parser.add_argument(
        "--auto-delay",
        type=float,
        default=3.0,
        help="Seconds to wait after subscriber connects before sending (default 3)",
    )
    parser.add_argument(
        "--idle-after",
        type=float,
        default=15.0,
        help="Seconds to idle after the test set completes (default 15)",
    )
    parser.add_argument(
        "--no-confirm",
        action="store_true",
        help="Clear REQUIRE CONFIRMATION on each buffer block (fire-and-forget)",
    )
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number {args.port} is out of range: must be 1 to {MAXPORT}")
        return 2

    status("=== STTP BufferBlock Interop Test Publisher (pyapi) ===")
    status(f"Port:        {args.port}")
    status(f"Signal ID:   {SIGNAL_ID}")
    status(f"Auto-count:  {args.auto_count} sets x {len(TEST_SET)} blocks = {args.auto_count * len(TEST_SET)} buffer blocks total")
    status(f"Auto-delay:  {args.auto_delay:.1f}s after subscribe")
    status(f"Require confirmation: {not args.no_confirm}")
    status("")

    publisher = Publisher()
    publisher.metadata_path = os.path.join(os.path.dirname(__file__), "Metadata.xml")

    err = publisher.load_metadata()
    if err is not None:
        status(f"ERROR loading metadata: {err}")
        return 1

    subscribed_event = Event()

    publisher.statusmessage_logger = lambda msg: status(f"[PUB] {msg}")
    publisher.errormessage_logger = lambda msg: status(f"[PUB][ERR] {msg}")

    def on_client_connected(connection: SubscriberConnection) -> None:
        status(f"<< CLIENT CONNECTED: {connection.connection_id}")

    def on_client_disconnected(connection: SubscriberConnection) -> None:
        status(f"<< CLIENT DISCONNECTED: {connection.connection_id}")

    publisher.clientconnected_receiver = on_client_connected
    publisher.clientdisconnected_receiver = on_client_disconnected

    publisher.start(args.port)

    status(f"Publisher started, listening on port {args.port}. Waiting for subscriber...")

    # Poll for an active subscription with a populated signal index cache - that confirms the
    # subscriber has issued a subscribe command and is ready to receive buffer blocks.
    deadline = time.time() + 60.0
    while time.time() < deadline:
        connections = list(publisher._datapublisher._subscriber_connections)
        if any(c._subscribed and c._signal_index_cache is not None for c in connections):
            subscribed_event.set()
            break
        time.sleep(0.2)

    if not subscribed_event.is_set():
        status("[ERR] No subscriber became ready within 60s; aborting.")
        publisher.dispose()
        return 3

    status(f"Subscriber is ready. Waiting {args.auto_delay:.1f}s before sending buffer blocks...")
    time.sleep(args.auto_delay)

    sent = 0
    for set_index in range(args.auto_count):
        status(f"  -- sending test set #{set_index + 1} of {args.auto_count} --")
        for label, payload in TEST_SET:
            count = publisher.broadcast_buffer_block(SIGNAL_ID, payload, require_confirmation=not args.no_confirm)
            sent += count
            status(f"  >> SENT [{label:<6s}] {len(payload)} bytes to {count} subscriber(s) (requireConfirmation={not args.no_confirm})")
        time.sleep(2.0)

    status(f"All {args.auto_count * len(TEST_SET)} buffer blocks queued (delivered to {sent} client(s) total).")
    status(f"Idling {args.idle_after:.1f}s to let the subscriber drain and observe...")
    time.sleep(args.idle_after)

    publisher.dispose()
    status("Publisher stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
