# ******************************************************************************************************
#  main.py - STTP Interop Single-Measurement Test Subscriber
#
#  Test harness to investigate "missing single measurement" issue between gsfapi (C# publisher) and
#  pyapi (Python subscriber).
#
#  Theory: When the gsfapi C# publisher sends a single measurement with TSSC compression enabled, the
#  measurement may not arrive at the Python subscriber. Run with --no-tssc to bypass TSSC and verify
#  the measurement arrives via the uncompressed CompactMeasurement format.
# ******************************************************************************************************

import argparse
import os
import sys
from datetime import datetime
from threading import Thread
from time import time
from typing import List

# Use the local sttp source tree at C:\Projects\sttp\pyapi\src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'pyapi', 'src')))

from gsf import Limits  # noqa: E402
from sttp.config import Config  # noqa: E402
from sttp.subscriber import Subscriber  # noqa: E402
from sttp.settings import Settings  # noqa: E402
from sttp.transport.measurement import Measurement  # noqa: E402

MAXPORT = Limits.MAXUINT16


def stamp() -> str:
    return datetime.now().strftime("%H:%M:%S.") + f"{datetime.now().microsecond // 1000:03d}"


def status(message: str) -> None:
    print(f"[{stamp()}] {message}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="STTP single-measurement interop test subscriber")
    parser.add_argument("--host", default="localhost", help="Publisher hostname (default localhost)")
    parser.add_argument("--port", type=int, default=7165, help="Publisher port (default 7165)")
    parser.add_argument("--no-tssc", action="store_true", help="Disable TSSC compression in subscription")
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
    args = parser.parse_args()

    if args.port < 1 or args.port > MAXPORT:
        print(f"Port number {args.port} is out of range: must be 1 to {MAXPORT}")
        return 2

    use_tssc = not args.no_tssc

    status("=== STTP Single-Measurement Test Subscriber ===")
    status(f"Publisher:                {args.host}:{args.port}")
    status(f"compress_payloaddata (TSSC): {use_tssc}")
    status(f"Subscription filter:      {args.filter}")
    status("")

    # Build configuration
    config = Config()
    config.compress_payloaddata = use_tssc
    config.compress_metadata = True
    config.compress_signalindexcache = True
    # Auto-reconnect off for diagnostics: we want to see drop behavior, not fight with reconnects
    config.autoreconnect = True

    settings = Settings()
    settings.usemillisecondresolution = False
    settings.includetime = True

    subscriber = Subscriber()

    # Wire up status / error / connection callbacks BEFORE subscribing
    subscriber.statusmessage_logger = lambda msg: status(f"[SUB] {msg}")
    subscriber.errormessage_logger = lambda msg: status(f"[SUB][ERR] {msg}")

    received_count = [0]
    last_value: List[float] = []

    def on_new_measurements(measurements: List[Measurement]) -> None:
        for m in measurements:
            received_count[0] += 1
            last_value.append(float(m.value))
            status(
                f"  << RECEIVED #{received_count[0]:>4d}: "
                f"signalid={m.signalid} ts={m.timestamp} value={float(m.value):.6f} flags={m.flags!s}"
            )

    def on_connection_established() -> None:
        status("[SUB] Connection established — measurement reader running on background thread")
        Thread(target=read_data, args=(subscriber,), name="ReadDataThread", daemon=True).start()

    def on_connection_terminated() -> None:
        status("[SUB] Connection terminated")

    def on_subscription_updated(cache) -> None:
        status(
            f"[SUB] Subscription updated: signal index cache has {cache.count if hasattr(cache, 'count') else 0} mappings"
        )

    subscriber.connectionestablished_receiver = on_connection_established
    subscriber.connectionterminated_receiver = on_connection_terminated
    subscriber.newmeasurements_receiver = on_new_measurements
    subscriber.subscriptionupdated_receiver = on_subscription_updated

    try:
        subscriber.subscribe(args.filter, settings)
        subscriber.connect(f"{args.host}:{args.port}", config)

        if args.timeout > 0:
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
        status(f"Total measurements received: {received_count[0]}")
    return 0


def read_data(subscriber: Subscriber) -> None:
    """Background loop that pulls measurements; not required since callback is wired, kept for parity with simplesubscribe."""
    last_summary = 0.0

    while subscriber.connected:
        if time() - last_summary >= 5.0:
            status(f"  ... still connected, total received so far: {subscriber.total_measurementsreceived:,}")
            last_summary = time()
        # tiny sleep keeps the thread cooperative
        try:
            from time import sleep

            sleep(0.5)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    sys.exit(main())
