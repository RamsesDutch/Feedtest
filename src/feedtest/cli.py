import argparse
import asyncio
import json
import math
from websockets.asyncio.server import serve
from websockets.exceptions import WebSocketException

from .core import SCENARIOS, load_capture, make_demo
from .runtime import probe, record, replay
from .kraken import KRAKEN_URL, kraken_handler, make_kraken_demo, prepare_capture, subscription


async def run_adapter(handler, port):
    async with serve(handler, "127.0.0.1", port):
        print(f"Replay listening on ws://127.0.0.1:{port} (Ctrl+C to stop)", flush=True)
        await asyncio.Future()


def main():
    parser = argparse.ArgumentParser(description="Local WebSocket feed failure tests")
    commands = parser.add_subparsers(dest="command", required=True)
    demo = commands.add_parser("demo", help="Create synthetic DEMO trades")
    demo.add_argument("output")
    demo.add_argument("--count", type=int, default=20)
    demo.add_argument("--adapter", choices=["raw", "kraken-book"], default="raw")
    demo.add_argument("--symbol", default="BTC/USD")
    server = commands.add_parser("replay")
    server.add_argument("capture")
    server.add_argument("--port", type=int, default=9000)
    server.add_argument("--scenario", choices=SCENARIOS, default="clean")
    server.add_argument("--at", type=int, default=5)
    server.add_argument("--pause", type=float, default=0.3)
    server.add_argument("--speed", type=float, default=1)
    server.add_argument("--adapter", choices=["raw", "kraken-book"], default="raw")
    server.add_argument("--symbol", default="BTC/USD")
    client = commands.add_parser("probe", help="Check synthetic DEMO trades only")
    client.add_argument("--url", default="ws://127.0.0.1:9000")
    client.add_argument("--naive", action="store_true")
    capture = commands.add_parser("record", help="Record raw public WebSocket frames")
    capture.add_argument("url")
    capture.add_argument("output")
    capture.add_argument("--seconds", type=float, default=10)
    capture.add_argument("--subscribe", help="UTF-8 file containing subscription message")
    kraken_capture = commands.add_parser("record-kraken", help="Record public Kraken v2 book data")
    kraken_capture.add_argument("output")
    kraken_capture.add_argument("--symbol", default="BTC/USD")
    kraken_capture.add_argument("--seconds", type=float, default=10)
    check = commands.add_parser("validate", help="Validate Kraken book checksums in a capture")
    check.add_argument("capture")
    check.add_argument("--symbol", default="BTC/USD")
    dashboard = commands.add_parser("dashboard", help="Launch the local browser dashboard")
    dashboard.add_argument("--port", type=int, default=8765)
    dashboard.add_argument("--open-browser", action="store_true")
    dashboard.add_argument(
        "--data-dir", default=".feedtest", help="Local captures and reports directory"
    )
    args = parser.parse_args()
    try:
        if args.command == "dashboard":
            from .dashboard import launch

            launch(args.data_dir, args.port, args.open_browser)
        elif args.command == "demo":
            if args.adapter == "kraken-book":
                make_kraken_demo(args.output, args.symbol)
                print(f"Created 8 synthetic Kraken book frames in {args.output}")
            else:
                make_demo(args.output, args.count)
                print(f"Created {args.count} synthetic trades in {args.output}")
        elif args.command == "replay":
            frames = load_capture(args.capture)
            options = dict(scenario=args.scenario, at=args.at, pause=args.pause, speed=args.speed)
            if args.adapter == "kraken-book":
                handler = kraken_handler(frames, args.symbol, **options)
                asyncio.run(run_adapter(handler, args.port))
            else:
                asyncio.run(replay(frames, args.port, **options))
        elif args.command == "probe":
            result = asyncio.run(probe(args.url, resilient=not args.naive))
            print("PASS " + json.dumps(result, sort_keys=True))
        elif args.command == "validate":
            frames = prepare_capture(load_capture(args.capture), args.symbol)
            print(f"PASS {len(frames)} book frames validated for {args.symbol}")
        else:
            if not math.isfinite(args.seconds) or args.seconds <= 0:
                raise ValueError("seconds must be finite and positive")
            subscription_message = None
            if args.command == "record-kraken":
                asyncio.run(
                    record(KRAKEN_URL, args.output, args.seconds, subscription(args.symbol))
                )
                prepare_capture(load_capture(args.output), args.symbol)
                print("PASS captured book checksums validated")
                return
            if args.subscribe:
                from pathlib import Path

                subscription_message = Path(args.subscribe).read_text(encoding="utf-8")
            asyncio.run(record(args.url, args.output, args.seconds, subscription_message))
    except KeyboardInterrupt:
        pass
    except (ValueError, OSError, TimeoutError, WebSocketException) as error:
        parser.exit(1, f"FAIL {error}\n")


if __name__ == "__main__":
    main()
