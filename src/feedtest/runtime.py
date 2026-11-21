"""WebSocket replay and a deliberately limited demo sequence checker."""

import asyncio
import json
import time
from urllib.parse import parse_qs, urlsplit

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from .core import Frame, save_frame, validate_scenario


def replay_handler(
    frames, scenario="clean", at=5, pause=0.3, speed=1, allow_cursor=True, on_disconnect=None
):
    validate_scenario(frames, scenario, at, pause, speed)

    async def handler(socket):
        # Each connection owns its cursor. Reconnect explicitly requests a frame
        # offset, avoiding shared cursors that mix separate clients' state.
        query = parse_qs(urlsplit(socket.request.path).query) if allow_cursor else {}
        try:
            start = int(query.get("start", ["0"])[0])
            if not 0 <= start <= len(frames):
                raise ValueError
        except ValueError:
            await socket.close(1008, "start must be a valid zero-based frame offset")
            return
        previous = frames[start].offset_ns if start < len(frames) else 0
        try:
            for index in range(start, len(frames)):
                frame = frames[index]
                await asyncio.sleep((frame.offset_ns - previous) / 1e9 / speed)
                previous = frame.offset_ns
                selected = index + 1 == at
                if selected and scenario == "gap":
                    continue
                if selected and scenario == "pause":
                    await asyncio.sleep(pause)
                await socket.send(frame.payload)
                if selected and scenario == "duplicate":
                    await socket.send(frame.payload)
                if selected and scenario == "disconnect":
                    if on_disconnect is not None:
                        on_disconnect()
                    # Graceful forced disconnect. No claim to emulate packet loss.
                    await socket.close(1012, "injected disconnect")
                    return
            await socket.close(1000, "replay complete")
        except ConnectionClosed:
            return

    return handler


async def replay(frames, port, **options):
    async with serve(replay_handler(frames, **options), "127.0.0.1", port):
        print(f"Replay listening on ws://127.0.0.1:{port} (Ctrl+C to stop)", flush=True)
        await asyncio.Future()


async def record(url, output, seconds, subscribe=None):
    # Exclusive creation protects captures from accidental overwrite. Frames are
    # flushed individually, so a stopped recording remains a usable prefix.
    count = 0
    with open(output, "x", encoding="utf-8") as stream:
        async with connect(url, proxy=None) as socket:
            if subscribe is not None:
                await socket.send(subscribe)
            start = time.monotonic_ns()
            deadline = asyncio.get_running_loop().time() + seconds
            while True:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    payload = await asyncio.wait_for(socket.recv(), remaining)
                except (TimeoutError, ConnectionClosed):
                    break
                save_frame(stream, Frame(time.monotonic_ns() - start, payload))
                count += 1
    if not count:
        raise ValueError("No messages captured; check the URL and subscription")
    print(f"Recorded {count} frames to {output}")


async def probe(url, resilient=True, stale_after=0.15):
    async with asyncio.timeout(10):
        return await _probe(url, resilient, stale_after)


async def _probe(url, resilient, stale_after):
    """Check only our synthetic contiguous sequence protocol, never venue data."""
    last = 0
    seen = 0
    duplicates = 0
    gaps = 0
    stale = 0
    reconnects = 0
    for attempt in range(4):
        try:
            async with connect(f"{url}?start={last}", proxy=None) as socket:
                while True:
                    try:
                        raw = await asyncio.wait_for(socket.recv(), stale_after)
                    except TimeoutError:
                        stale += 1
                        if not resilient:
                            raise ValueError("stale feed")
                        continue
                    event = json.loads(raw)
                    if event.get("symbol") != "DEMO":
                        raise ValueError("probe only supports the synthetic DEMO protocol")
                    sequence = event["sequence"]
                    if sequence <= last:
                        duplicates += 1
                        if not resilient:
                            raise ValueError("duplicate sequence")
                        continue
                    if sequence != last + 1:
                        gaps += 1
                        # Detection isn't recovery: return failure until resynced.
                        raise ValueError(f"unrecovered gap: expected {last + 1}, got {sequence}")
                    last = sequence
                    seen += 1
        except ConnectionClosed as error:
            if error.rcvd is not None and error.rcvd.code == 1000:
                if not seen:
                    raise ValueError("empty replay")
                return {
                    "unique": seen,
                    "duplicates": duplicates,
                    "gaps": gaps,
                    "stale": stale,
                    "reconnects": reconnects,
                }
            if not resilient:
                raise ValueError("connection lost") from error
            reconnects += 1
            await asyncio.sleep(0.02)
    raise ValueError("reconnect limit exceeded")
