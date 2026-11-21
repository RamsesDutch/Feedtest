"""Embed a local replay server in asynchronous tests."""

from contextlib import asynccontextmanager

from websockets.asyncio.server import serve

from .core import load_capture
from .kraken import kraken_handler
from .runtime import replay_handler


@asynccontextmanager
async def serve_capture(path, *, adapter="raw", symbol="BTC/USD", port=0, **scenario):
    """Yield a running localhost server; port=0 allocates an available port.

    Use server.sockets[0].getsockname()[1] to obtain its selected port.
    The context closes connections and releases its port even on test failure.
    """
    frames = load_capture(path)
    if adapter == "kraken-book":
        handler = kraken_handler(frames, symbol, **scenario)
    elif adapter == "raw":
        handler = replay_handler(frames, **scenario)
    else:
        raise ValueError(f"unknown adapter: {adapter}")
    async with serve(handler, "127.0.0.1", port) as server:
        yield server
