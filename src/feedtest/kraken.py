"""Kraken Spot WebSocket v2 book adapter (one symbol, depth 10).

Protocol reference: https://docs.kraken.com/exchange/api-reference/spot-websocket-v2/book
Checksum reference: https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
"""

import asyncio
import json
import re
import zlib
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qs, urlsplit

from websockets.exceptions import ConnectionClosed

from .core import Frame, save_frame, validate_scenario
from .runtime import replay_handler


KRAKEN_URL = "wss://ws.kraken.com/v2"


def subscription(symbol):
    return json.dumps(
        {
            "method": "subscribe",
            "params": {"channel": "book", "symbol": [symbol], "depth": 10, "snapshot": True},
        }
    )


def parse_message(payload):
    try:
        message = json.loads(payload, parse_float=Decimal)
    except (ValueError, UnicodeError) as error:
        raise ValueError("invalid Kraken JSON") from error
    if not isinstance(message, dict):
        raise ValueError("Kraken message must be an object")
    return message


def number(value, positive=False):
    if isinstance(value, (bool, float)):
        raise ValueError("prices and quantities must preserve decimal precision")
    try:
        result = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("invalid price or quantity") from error
    if not result.is_finite() or result < 0 or (positive and result == 0):
        raise ValueError("price must be positive and quantity nonnegative")
    return result


def checksum(asks, bids):
    parts = []
    for side, reverse in ((asks, False), (bids, True)):
        for price in sorted(side, reverse=reverse)[:10]:
            for value in (price, side[price]):
                parts.append(format(value, "f").replace(".", "").lstrip("0"))
    return zlib.crc32("".join(parts).encode("ascii")) & 0xFFFFFFFF


class Book:
    """Publish state only after the whole message has passed CRC validation."""

    def __init__(self, symbol):
        self.symbol = symbol
        self.asks = {}
        self.bids = {}
        self.ready = False
        self.messages = 0

    def apply(self, message):
        if message.get("channel") != "book":
            return False
        # A failed validation invalidates the old book as well. A new snapshot
        # is required before publishing another usable state.
        was_ready = self.ready
        self.ready = False
        kind = message.get("type")
        if kind not in ("snapshot", "update"):
            raise ValueError("unsupported book message type")
        if kind == "update" and not was_ready:
            raise ValueError("update requires a validated snapshot")
        data = message.get("data")
        if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
            raise ValueError("adapter requires exactly one book entry per frame")
        entry = data[0]
        if entry.get("symbol") != self.symbol:
            raise ValueError("unexpected symbol")
        asks = {} if kind == "snapshot" else self.asks.copy()
        bids = {} if kind == "snapshot" else self.bids.copy()
        for label, side, reverse in (("asks", asks, False), ("bids", bids, True)):
            levels = entry.get(label, [])
            if not isinstance(levels, list):
                raise ValueError("book levels must be arrays")
            for level in levels:
                if not isinstance(level, dict) or "price" not in level or "qty" not in level:
                    raise ValueError("invalid book level")
                price = number(level["price"], positive=True)
                quantity = number(level["qty"])
                if quantity == 0:
                    side.pop(price, None)
                else:
                    # Replacing a Decimal key also retains the latest wire
                    # representation for the checksum's decimal formatting.
                    side.pop(price, None)
                    side[price] = quantity
            keep = sorted(side, reverse=reverse)[:10]
            for price in set(side).difference(keep):
                del side[price]
        expected = entry.get("checksum")
        if type(expected) is not int or not 0 <= expected <= 0xFFFFFFFF:
            raise ValueError("missing or invalid checksum")
        actual = checksum(asks, bids)
        if actual != expected:
            raise ValueError(f"checksum mismatch: expected {expected}, computed {actual}")
        if not asks or not bids or max(bids) >= min(asks):
            raise ValueError("empty or crossed book")
        self.asks, self.bids = asks, bids
        self.ready = True
        self.messages += 1
        return True


def prepare_capture(frames, symbol):
    """Validate the baseline before injecting faults; remove control frames."""
    book = Book(symbol)
    selected = []
    for index, frame in enumerate(frames, 1):
        try:
            message = parse_message(frame.payload)
            if message.get("success") is False:
                raise ValueError("capture contains a rejected subscription")
            if book.apply(message):
                selected.append(frame)
        except ValueError as error:
            raise ValueError(f"capture frame {index}: {error}") from error
    if not selected:
        raise ValueError("capture has no validated book snapshot")
    return selected


def kraken_handler(frames, symbol="BTC/USD", scenario="clean", at=3, pause=0.5, speed=1):
    selected = prepare_capture(frames, symbol)
    validate_scenario(selected, scenario, at, pause, speed)
    if scenario != "clean" and at < 2:
        raise ValueError("fault must select an update after the initial snapshot")
    if scenario != "clean" and parse_message(selected[at - 1].payload)["type"] != "update":
        raise ValueError("fault must select an update frame")
    # Explicit session names isolate reconnect tests. Bound retained state and
    # reject excess sessions rather than evicting an active client's history.
    disconnected_sessions = set()
    known_sessions = set()

    async def handler(socket):
        query = parse_qs(urlsplit(socket.request.path).query)
        sessions = query.get("session", [])
        session = sessions[0] if len(sessions) == 1 else ""
        if scenario == "disconnect" and (not session or len(session) > 128):
            await socket.close(1008, "disconnect scenario requires ?session=NAME")
            return
        try:
            raw = await asyncio.wait_for(socket.recv(), 5)
            request = parse_message(raw)
            params = request.get("params", {})
            if not isinstance(params, dict):
                raise ValueError("params must be an object")
            if (
                request.get("method") != "subscribe"
                or params.get("channel") != "book"
                or params.get("symbol") != [symbol]
                or params.get("depth", 10) != 10
                or params.get("snapshot", True) is not True
            ):
                raise ValueError(
                    "supported subscription: book, one configured symbol, depth 10, snapshot true"
                )
            if scenario == "disconnect" and session not in known_sessions:
                if len(known_sessions) >= 128:
                    raise ValueError("session limit reached; restart replay server")
                known_sessions.add(session)
            response = {
                "method": "subscribe",
                "success": True,
                "result": {"channel": "book", "symbol": symbol, "depth": 10, "snapshot": True},
            }
            if "req_id" in request:
                response["req_id"] = request["req_id"]
            await socket.send(json.dumps(response))
            # Every subscription starts with a fresh snapshot. One injected
            # disconnect per explicit session permits a complete recovery run.
            effective = scenario
            if scenario == "disconnect":
                if session in disconnected_sessions:
                    effective = "clean"
            await replay_handler(
                selected,
                effective,
                at,
                pause,
                speed,
                allow_cursor=False,
                on_disconnect=lambda: disconnected_sessions.add(session),
            )(socket)
        except (ValueError, TimeoutError) as error:
            await socket.close(1008, str(error)[:100])
        except ConnectionClosed:
            return

    return handler


def make_kraken_demo(path, symbol="BTC/USD"):
    """Small authored fixture in exchange format; no redistributed market data."""
    asks = {Decimal("101.0"): Decimal("1.00000000")}
    bids = {Decimal("99.0"): Decimal("2.00000000")}
    with open(path, "x", encoding="utf-8") as stream:
        for index in range(8):
            side = asks if index % 2 else bids
            price = next(iter(side))
            if index:
                side[price] = Decimal(index + 2)
            data = {
                "symbol": symbol,
                "checksum": checksum(asks, bids),
                "timestamp": f"2026-01-01T00:00:00.{index * 50000:06d}Z",
            }
            if index == 0:
                data["asks"] = [{"price": str(p), "qty": str(q)} for p, q in asks.items()]
                data["bids"] = [{"price": str(p), "qty": str(q)} for p, q in bids.items()]
            else:
                label = "asks" if index % 2 else "bids"
                data[label] = [{"price": str(price), "qty": str(side[price])}]
            # Numeric fields on the wire, preserving the authored decimal text.
            raw = json.dumps(
                {"channel": "book", "type": "snapshot" if index == 0 else "update", "data": [data]}
            )
            raw = re.sub(r'"(price|qty)": "([0-9.]+)"', r'"\1": \2', raw)
            save_frame(stream, Frame(index * 50_000_000, raw))
