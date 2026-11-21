# FeedTest

Reproduce WebSocket market-data failures locally and test how your client reacts.

FeedTest records raw frames, replays them, and injects a duplicate, omitted
frame, pause, or forced disconnect at a chosen frame. Version 0.2 adds a
Kraken Spot WebSocket v2 order-book adapter with subscription handling and
checksum validation. An independent example consumer verifies the resulting
book and rebuilds it after reconnecting.

Version 0.3 adds a local browser dashboard for capture selection, checks,
external replay, public recording, and exported reports.

The offline demo uses authored synthetic Kraken-format data. No account,
API key, or market-data download is required.

## Start the dashboard

From the repository folder in Windows Command Prompt

```bat
py -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
.venv\Scripts\python.exe -m pytest -q
.venv\Scripts\python.exe -m feedtest dashboard --open-browser
```

Open **http://127.0.0.1:8765** in your browser. Keep the single launch terminal
open. Select the bundled synthetic demo, leave “All five checks” selected,
and click **Run checks**. Expect **5 / 5**. The gap and pause checks pass when
the client rejects the bad feed for the expected reason.

Choose **Disconnect and resubscribe**, then **Start external replay** to get a
WebSocket URL for your own client. Click **Stop operations** to cancel work
and close that external replay. **Export JSON** downloads evidence from the
latest completed run. Previous completed reports survive restarts and can
be downloaded from **Previous reports**.

Use the file picker to import a Kraken JSONL capture. Expand the public-feed
section to record from Kraken. This optional operation needs internet access.
The bundled demo and tests work offline after dependencies are installed.

The default data folder is `.feedtest` under the current working directory,
excluded by `.gitignore`. Use `--data-dir PATH` to choose another folder and
`--port 8766` if 8765 is occupied. The dashboard binds only to 127.0.0.1.
Closing the browser does not stop the process; press Ctrl+C in its terminal.
See [dashboard behavior and test instructions](docs/dashboard.md).

Linux/macOS users can activate their virtual environment and run
`python -m feedtest dashboard` with the same browser workflow.

## Quick start on Windows

Extract the project and open Command Prompt inside the folder containing
`pyproject.toml`. Use a new folder for this release so your existing captures
and environment stay available. Install Python 3.11 or newer, then run

```bat
py -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
.venv\Scripts\python.exe -m pytest -q
.venv\Scripts\python.exe -m feedtest.cli demo kraken.jsonl --adapter kraken-book
.venv\Scripts\python.exe -m feedtest.cli validate kraken.jsonl
.venv\Scripts\python.exe -m feedtest.cli replay kraken.jsonl --adapter kraken-book
```

Expect `PASS 8 book frames validated for BTC/USD` before starting the server.
Leave the server running. Open another Command Prompt in the same folder

```bat
.venv\Scripts\python.exe examples\kraken_book_client.py
```

Expected output

```text
PASS {"best_ask": "101.0", "best_bid": "99.0", "final_connection_frames": 8, "reconnects": 0, "validated_frames": 8}
```

## Quick start on Linux/macOS

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[dev]"
python -m pytest -q
feedtest demo kraken.jsonl --adapter kraken-book
feedtest validate kraken.jsonl
feedtest replay kraken.jsonl --adapter kraken-book
```

In a second terminal, from the same folder

```sh
.venv/bin/python examples/kraken_book_client.py
```

## Run failure scenarios

Stop the server with Ctrl+C before changing scenarios. The following Windows
commands replace the clean server. Keep using the standalone example client
in the second window. Linux/macOS users can use `feedtest` after activation.

### Recover from disconnect

```bat
.venv\Scripts\python.exe -m feedtest.cli replay kraken.jsonl --adapter kraken-book --scenario disconnect --at 3
```

Run the client with a unique session name for this test run

```bat
.venv\Scripts\python.exe examples\kraken_book_client.py --url "ws://127.0.0.1:9000?session=run1"
```

Expect `reconnects` to be 1, `validated_frames` to be 11, and
`final_connection_frames` to be 8. It validated 3 frames before disconnect,
discarded the old book, then validated all 8 frames from a new snapshot.
The second pass replays the recording from its beginning; it does not simulate
the exchange's advancing wall clock. The session parameter is a FeedTest
extension. One disconnect is injected per session for the server's lifetime.
Use `run2` for another run, or restart the server. Give each concurrent client
its own session. At most 128 session identities are retained per server.

### Detect damage from a missing update

```bat
.venv\Scripts\python.exe -m feedtest.cli replay kraken.jsonl --adapter kraken-book --scenario gap --at 3
```

Run the example client. Expect exit code 1 and
`FAIL checksum mismatch; book invalid, new snapshot required`.
The omitted update changes a bid; the next update changes an ask, so the
next checksum detects the missing bid change.

### Replay a duplicate

```bat
.venv\Scripts\python.exe -m feedtest.cli replay kraken.jsonl --adapter kraken-book --scenario duplicate --at 3
```

Expect 9 validated frames and 0 reconnects. These book updates set absolute
quantities. Applying the same update twice leaves the same book. There is no
invented contiguous sequence-number rule or trade-style duplicate counter.

### Detect a stale stream

```bat
.venv\Scripts\python.exe -m feedtest.cli replay kraken.jsonl --adapter kraken-book --scenario pause --at 3 --pause 3
```

Run the example client. Its default receive timeout is 2 seconds, so expect
exit code 1 and `FAIL stale feed; book invalid`. The demo fails closed on
staleness and checksum errors. It only automatically recovers from transport
disconnects by resubscribing and rebuilding. Timeouts depend on host scheduling.

## Record public Kraken data

```bat
.venv\Scripts\python.exe -m feedtest.cli record-kraken public-book.jsonl --seconds 10 --symbol BTC/USD
.venv\Scripts\python.exe -m feedtest.cli validate public-book.jsonl --symbol BTC/USD
.venv\Scripts\python.exe -m feedtest.cli replay public-book.jsonl --adapter kraken-book --symbol BTC/USD
```

This command connects to `wss://ws.kraken.com/v2`, subscribes to public book
data, and validates the captured baseline. Output paths must be new. The raw
recording is retained even if final validation fails, so it can be inspected.
External connectivity and regional exchange availability can affect this
optional workflow.

The separate example client can also connect directly to Kraken using
`--url wss://ws.kraken.com/v2 --timeout 10`. A live stream does not normally end
after a fixed number of frames. Its deadline intentionally exits with `FAIL
overall timeout`; use recordings for repeatable completion assertions.

## Use it with your application

Point a Kraken v2 book client's configurable WebSocket URL at
`ws://127.0.0.1:9000`. Send a subscription for one configured symbol, `book`,
depth 10, snapshot true. For the disconnect scenario, add a unique
`?session=CLIENT_NAME` to that local URL and reuse it when reconnecting.

The adapter accepts the first supported subscription, acknowledges it, and
replays a validated snapshot and updates. It implements this documented subset,
including optional `req_id` echo. It does not emulate the complete Kraken
service, authentication, instrument discovery, unsubscribe, or application-level
ping requests. WebSocket control ping/pong is handled by the transport library.
It sends no periodic heartbeat messages. A client requiring extra methods
needs additional adapter work.

The wrapper in `examples/kraken_book_client.py` starts `feedtest.example_client`.
That consumer implementation depends only on stdlib and websockets, and
imports no replay or adapter validation code. Its parsing, book storage,
and CRC calculation are separate.
It demonstrates interoperability with the supported subset, not compatibility
with every third-party Kraken SDK.

## Embed replay in pytest

```python
import asyncio
from feedtest.api import serve_capture


def test_my_consumer():
    async def check():
        async with serve_capture(
            "kraken.jsonl", adapter="kraken-book", scenario="gap", at=3
        ) as server:
            port = server.sockets[0].getsockname()[1]
            url = f"ws://127.0.0.1:{port}"
            # Connect your consumer to url, then assert its invalid-book state.

    asyncio.run(check())
```

The snippet shows server lifecycle only. Add your consumer and its assertion.
See `tests/test_kraken.py` for complete integration tests with actual assertions.
Ephemeral ports prevent parallel tests from competing for port 9000.

## Raw replay and original demo

Existing raw commands remain available. Use `feedtest demo demo.jsonl` and
`feedtest probe` for the original contiguous-sequence DEMO protocol.
See [the raw replay walkthrough](docs/raw-replay.md).
`feedtest record URL capture.jsonl --subscribe subscription.json` sends one
UTF-8 subscription message and records text or binary frames. Raw replay sends
captured frames immediately after connection. It does not interpret requests.

## Format and limits

Captures are JSONL rows with `offset_ns`, `binary`, and `payload`. Offsets are
nonnegative monotonic receive-time offsets. Binary payloads use base64.
Existing 0.1 recordings remain readable. Kraken preparation removes control
messages and validates every book message before injecting any fault. Fault
`--at` values are 1-based positions among selected book frames, including the
initial snapshot. Use `--at 3` with the bundled fixture. Faults must target
updates. Original raw mode indexes every recorded frame instead.

All local servers bind to 127.0.0.1. Captures load into memory; use modest
files. Fault locations and payload order are deterministic. Delivery timing
uses asyncio and the host scheduler; this is not a precision latency benchmark.
Forced disconnect uses close code 1012 and a closing handshake, not packet loss.
Stale detection in the example measures time between received messages, not
exchange timestamp age. Heartbeats on live feeds may keep that timeout alive.

CRC validation covers the top 10 levels. It does not prove every event was
received: a lost update can be superseded before the next check, or concern a
level outside the checksum scope. A missing final frame may be undetectable.
FeedTest's `gap` scenario omits a frame; Kraken's book feed has no contiguous
sequence field used by this adapter. On validation failure, `Book.ready` is
false until a valid new snapshot is applied. Consumers must honor that flag.

## Development and testing

Install the development dependencies and run the checks from the repository root

```sh
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
python -m build
```

The default suite covers replay, checksum validation, client recovery, dashboard
operations, persistence, and input validation. The optional Playwright suite
checks the browser interface. See [browser test instructions](docs/dashboard.md#browser-tests).

GitHub Actions runs tests on Windows and Ubuntu with Python 3.11–3.13, plus a
Chromium browser job. Live exchange access is optional and is excluded from CI.

See [contribution instructions](CONTRIBUTING.md), [changes](CHANGELOG.md),
and [protocol references](docs/protocol.md). Licensed under [MIT](LICENSE).
