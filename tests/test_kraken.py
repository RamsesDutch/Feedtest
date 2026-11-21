import asyncio
import importlib.util
from decimal import Decimal
from pathlib import Path

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from feedtest.api import serve_capture
from feedtest.core import Frame, load_capture
from feedtest.kraken import (
    Book,
    checksum,
    kraken_handler,
    make_kraken_demo,
    parse_message,
    prepare_capture,
)


spec = importlib.util.spec_from_file_location(
    "independent_client", Path(__file__).parents[1] / "examples" / "kraken_book_client.py"
)
client = importlib.util.module_from_spec(spec)
spec.loader.exec_module(client)


@pytest.fixture
def capture(tmp_path):
    path = tmp_path / "kraken.jsonl"
    make_kraken_demo(path)
    return path


def test_published_kraken_crc_vector():
    # Kraken's published checksum example, accessed 2026-11-21.
    # https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2
    asks = dict(
        (Decimal(p), Decimal(q))
        for p, q in [
            ("45285.2", "0.00100000"),
            ("45286.4", "1.54571953"),
            ("45286.6", "1.54571109"),
            ("45289.6", "1.54560911"),
            ("45290.2", "0.15890660"),
            ("45291.8", "1.54553491"),
            ("45294.7", "0.04454749"),
            ("45296.1", "0.35380000"),
            ("45297.5", "0.09945542"),
            ("45299.5", "0.18772827"),
        ]
    )
    bids = dict(
        (Decimal(p), Decimal(q))
        for p, q in [
            ("45283.5", "0.10000000"),
            ("45283.4", "1.54582015"),
            ("45282.1", "0.10000000"),
            ("45281.0", "0.10000000"),
            ("45280.3", "1.54592586"),
            ("45279.0", "0.07990000"),
            ("45277.6", "0.03310103"),
            ("45277.5", "0.30000000"),
            ("45277.3", "1.54602737"),
            ("45276.6", "0.15445238"),
        ]
    )
    assert checksum(asks, bids) == 3310070434


def test_baseline_and_precision(capture):
    frames = prepare_capture(load_capture(capture), "BTC/USD")
    assert len(frames) == 8
    message = parse_message(frames[0].payload)
    assert message["data"][0]["asks"][0]["qty"].as_tuple().exponent == -8


@pytest.mark.parametrize("scenario", ["clean", "duplicate", "disconnect"])
def test_independent_client_replay_and_fresh_snapshot(capture, scenario):
    async def run():
        async with serve_capture(
            capture, adapter="kraken-book", scenario=scenario, at=3, speed=20
        ) as server:
            port = server.sockets[0].getsockname()[1]
            result = await client.run(f"ws://127.0.0.1:{port}?session=first")
            assert result["final_connection_frames"] == (9 if scenario == "duplicate" else 8)
            assert result["reconnects"] == (1 if scenario == "disconnect" else 0)
            assert result["validated_frames"] == (
                11 if scenario == "disconnect" else result["final_connection_frames"]
            )
            assert result["best_bid"] == "99.0"
            assert result["best_ask"] == "101.0"

    asyncio.run(run())


def test_missing_update_breaks_checksum(capture):
    async def run():
        async with serve_capture(
            capture, adapter="kraken-book", scenario="gap", at=3, speed=20
        ) as server:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(ValueError, match="checksum mismatch"):
                await client.run(f"ws://127.0.0.1:{port}")

    asyncio.run(run())


def test_stale_book_fails(capture):
    async def run():
        async with serve_capture(
            capture, adapter="kraken-book", scenario="pause", pause=0.8, at=3, speed=20
        ) as server:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(ValueError, match="stale feed"):
                await client.run(f"ws://127.0.0.1:{port}", stale_after=0.2)

    asyncio.run(run())


def test_rejected_subscription_and_missing_session(capture):
    async def run():
        handler = kraken_handler(load_capture(capture), scenario="disconnect", at=3)
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            async with connect(f"ws://127.0.0.1:{port}", proxy=None) as socket:
                with pytest.raises(ConnectionClosed) as error:
                    await socket.recv()
                assert error.value.rcvd.code == 1008
            with pytest.raises(ValueError, match="subscription rejected"):
                await client.run(f"ws://127.0.0.1:{port}?session=bad", symbol="ETH/USD")

    asyncio.run(run())


def test_sessions_are_isolated(capture):
    async def run():
        async with serve_capture(
            capture, adapter="kraken-book", scenario="disconnect", at=3, speed=50
        ) as server:
            port = server.sockets[0].getsockname()[1]
            results = await asyncio.gather(
                *[client.run(f"ws://127.0.0.1:{port}?session={name}") for name in ("a", "b")]
            )
            assert [result["reconnects"] for result in results] == [1, 1]

    asyncio.run(run())


def test_invalid_book_is_latched_until_snapshot(capture):
    frames = load_capture(capture)
    book = Book("BTC/USD")
    book.apply(parse_message(frames[0].payload))
    old_asks = book.asks.copy()
    broken = parse_message(frames[1].payload)
    broken["data"][0]["checksum"] ^= 1
    with pytest.raises(ValueError, match="checksum mismatch"):
        book.apply(broken)
    assert not book.ready
    assert book.asks == old_asks
    with pytest.raises(ValueError, match="snapshot"):
        book.apply(parse_message(frames[2].payload))
    book.apply(parse_message(frames[0].payload))
    assert book.ready


def test_delete_repeated_level_and_depth_truncation(capture):
    book = Book("BTC/USD")
    book.apply(parse_message(load_capture(capture)[0].payload))
    asks = {Decimal(str(101 + i)): Decimal("1") for i in range(10)}
    asks.pop(Decimal("101"))
    asks[Decimal("101.0")] = Decimal("1")
    bids = {Decimal("99.0"): Decimal("2.00000000")}
    levels = [{"price": str(101 + i), "qty": "1"} for i in range(11)]
    levels += [{"price": "101.0", "qty": "3"}, {"price": "101.0", "qty": "1"}]
    book.apply(
        {
            "channel": "book",
            "type": "update",
            "data": [{"symbol": "BTC/USD", "asks": levels, "checksum": checksum(asks, bids)}],
        }
    )
    assert len(book.asks) == 10
    assert Decimal("111") not in book.asks
    asks.pop(Decimal("101"))
    book.apply(
        {
            "channel": "book",
            "type": "update",
            "data": [
                {
                    "symbol": "BTC/USD",
                    "asks": [{"price": "101", "qty": "0"}],
                    "checksum": checksum(asks, bids),
                }
            ],
        }
    )
    assert min(book.asks) == Decimal("102")


def test_invalid_baseline_is_rejected_before_serving(capture):
    frames = load_capture(capture)
    with pytest.raises(ValueError, match="snapshot"):
        kraken_handler(frames[1:])
    with pytest.raises(ValueError, match="after the initial snapshot"):
        kraken_handler(frames, scenario="gap", at=1)
    with pytest.raises(ValueError, match="unexpected symbol"):
        kraken_handler(frames, symbol="ETH/USD")
    with pytest.raises(ValueError, match="invalid Kraken JSON"):
        prepare_capture([Frame(0, "bad")], "BTC/USD")
