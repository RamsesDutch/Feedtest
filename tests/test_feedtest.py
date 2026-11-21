import asyncio
import json

import pytest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve

from feedtest.core import Frame, load_capture, make_demo
from feedtest.runtime import probe, record, replay_handler


def demo_frames(tmp_path):
    path = tmp_path / "demo.jsonl"
    make_demo(path)
    return load_capture(path)


@pytest.mark.parametrize("scenario", ["clean", "duplicate", "pause", "disconnect"])
def test_resilient_client(tmp_path, scenario):
    async def run():
        handler = replay_handler(demo_frames(tmp_path), scenario, pause=0.5, speed=10)
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            result = await probe(f"ws://127.0.0.1:{port}", stale_after=0.1)
        assert result["unique"] == 20
        assert result["duplicates"] == (1 if scenario == "duplicate" else 0)
        assert result["reconnects"] == (1 if scenario == "disconnect" else 0)
        # Scheduling delays can trigger timeouts in any scenario. Only the
        # deliberate pause promises a stale observation.
        if scenario == "pause":
            assert result["stale"] > 0

    asyncio.run(run())


@pytest.mark.parametrize("scenario", ["duplicate", "gap", "pause", "disconnect"])
def test_naive_client_catches_fault(tmp_path, scenario):
    async def run():
        handler = replay_handler(demo_frames(tmp_path), scenario, pause=0.5, speed=10)
        expected_error = {
            "duplicate": "duplicate sequence",
            "gap": "unrecovered gap",
            "pause": "stale feed",
            "disconnect": "connection lost",
        }
        stale_after = 0.1 if scenario == "pause" else 2.0
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(ValueError, match=expected_error[scenario]):
                await probe(f"ws://127.0.0.1:{port}", resilient=False, stale_after=stale_after)

    asyncio.run(run())


def test_gap_is_never_reported_as_recovered(tmp_path):
    async def run():
        async with serve(
            replay_handler(demo_frames(tmp_path), "gap", speed=10), "127.0.0.1", 0
        ) as server:
            port = server.sockets[0].getsockname()[1]
            with pytest.raises(ValueError, match="unrecovered gap"):
                await probe(f"ws://127.0.0.1:{port}")

    asyncio.run(run())


def test_record_roundtrip_binary_text_and_no_overwrite(tmp_path):
    async def run():
        frames = [Frame(0, "hello"), Frame(1, b"\x00\xff")]
        path = tmp_path / "recording.jsonl"
        async with serve(replay_handler(frames), "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            await record(f"ws://127.0.0.1:{port}", path, 1)
        assert [f.payload for f in load_capture(path)] == [f.payload for f in frames]
        with pytest.raises(FileExistsError):
            await record("ws://127.0.0.1:1", path, 1)

    asyncio.run(run())


def test_clients_have_independent_cursors_and_repeatable_output(tmp_path):
    async def run():
        handler = replay_handler(demo_frames(tmp_path), "duplicate", speed=100)
        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]

            async def collect():
                async with connect(f"ws://127.0.0.1:{port}", proxy=None) as socket:
                    return [json.loads(raw)["sequence"] async for raw in socket]

            first, second = await asyncio.gather(collect(), collect())
            assert first == second == list(range(1, 6)) + list(range(5, 21))

    asyncio.run(run())


@pytest.mark.parametrize("content", ["", "{}\n", '{"offset_ns":-1}\n', "bad json\n"])
def test_invalid_captures(tmp_path, content):
    path = tmp_path / "bad.jsonl"
    path.write_text(content)
    with pytest.raises(ValueError):
        load_capture(path)


@pytest.mark.parametrize(
    "options",
    [
        {"speed": 0},
        {"speed": float("nan")},
        {"scenario": "gap", "at": 99},
        {"scenario": "disconnect", "at": 20},
    ],
)
def test_invalid_scenario(tmp_path, options):
    with pytest.raises(ValueError):
        replay_handler(demo_frames(tmp_path), **options)
