"""Standalone Kraken v2 book consumer. Uses only stdlib + websockets.

This file deliberately imports no FeedTest implementation. Point --url at a
FeedTest adapter or Kraken's public endpoint. Every reconnect discards the
previous book and waits for a fresh snapshot.
"""

import argparse
import asyncio
import json
import zlib
from decimal import Decimal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, WebSocketException


def consume(message, asks, bids, ready, symbol):
    if message.get("success") is False:
        raise ValueError("subscription rejected")
    if message.get("channel") != "book":
        return ready, False
    kind = message.get("type")
    if kind == "snapshot":
        asks.clear()
        bids.clear()
    elif kind != "update" or not ready:
        raise ValueError("update without snapshot")
    data = message["data"]
    if len(data) != 1 or data[0]["symbol"] != symbol:
        raise ValueError("unexpected symbol or book entry count")
    entry = data[0]
    for label, side, descending in (("asks", asks, False), ("bids", bids, True)):
        for level in entry.get(label, []):
            price, qty = Decimal(level["price"]), Decimal(level["qty"])
            if not price.is_finite() or not qty.is_finite() or price <= 0 or qty < 0:
                raise ValueError("invalid price or quantity")
            side.pop(price, None)
            if qty:
                side[price] = qty
        for price in sorted(side, reverse=descending)[10:]:
            del side[price]
    content = ""
    for side, descending in ((asks, False), (bids, True)):
        for price in sorted(side, reverse=descending)[:10]:
            content += format(price, "f").replace(".", "").lstrip("0")
            content += format(side[price], "f").replace(".", "").lstrip("0")
    crc = zlib.crc32(content.encode("ascii")) & 0xFFFFFFFF
    if type(entry.get("checksum")) is not int or crc != entry["checksum"]:
        raise ValueError("checksum mismatch; book invalid, new snapshot required")
    if not asks or not bids or max(bids) >= min(asks):
        raise ValueError("empty or crossed book")
    return True, True


async def run(url, symbol="BTC/USD", stale_after=2, timeout=15):
    reconnects = 0
    total = 0
    async with asyncio.timeout(timeout):
        for attempt in range(4):
            asks, bids = {}, {}
            ready = False
            current = 0
            try:
                async with connect(url, proxy=None) as socket:
                    await socket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "req_id": 1,
                                "params": {
                                    "channel": "book",
                                    "symbol": [symbol],
                                    "depth": 10,
                                    "snapshot": True,
                                },
                            }
                        )
                    )
                    while True:
                        try:
                            raw = await asyncio.wait_for(socket.recv(), stale_after)
                        except TimeoutError as error:
                            raise ValueError("stale feed; book invalid") from error
                        message = json.loads(raw, parse_float=Decimal)
                        ready, applied = consume(message, asks, bids, ready, symbol)
                        if applied:
                            total += 1
                            current += 1
            except ConnectionClosed as error:
                if error.rcvd is not None and error.rcvd.code == 1000:
                    if not ready or not current:
                        raise ValueError("replay ended without a validated book")
                    return {
                        "validated_frames": total,
                        "final_connection_frames": current,
                        "reconnects": reconnects,
                        "best_bid": str(max(bids)),
                        "best_ask": str(min(asks)),
                    }
                # A policy/protocol rejection needs fixing, not blind retries.
                if error.rcvd is not None and error.rcvd.code == 1008:
                    raise ValueError(f"subscription rejected: {error.rcvd.reason}") from error
                reconnects += 1
                await asyncio.sleep(0.1)
    raise ValueError("reconnect limit exceeded")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="ws://127.0.0.1:9000?session=example")
    parser.add_argument("--symbol", default="BTC/USD")
    parser.add_argument("--stale-after", type=float, default=2)
    parser.add_argument("--timeout", type=float, default=15)
    args = parser.parse_args()
    import math

    if not all(math.isfinite(value) and value > 0 for value in (args.stale_after, args.timeout)):
        parser.error("timeouts must be finite and positive")
    try:
        result = asyncio.run(run(args.url, args.symbol, args.stale_after, args.timeout))
        print("PASS " + json.dumps(result, sort_keys=True))
    except (ValueError, KeyError, TypeError, OSError, TimeoutError, WebSocketException) as error:
        parser.exit(1, f"FAIL {error or 'overall timeout'}\n")
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
