# Raw replay and synthetic feed walkthrough

A local WebSocket recorder and replay server with reproducible failure scenarios.
Use it to test a feed consumer's duplicate handling, gap detection, stale-data
detection, and reconnect behavior. Includes a synthetic trade feed and two demo
client modes so you can see a failure before integrating your own application.

## Windows setup, step by step

1. Install Python 3.11 or newer. Confirm `py --version` in Command Prompt.
2. Extract the ZIP. Open Command Prompt inside the extracted `feedtest` folder
   containing this README and `pyproject.toml`. In Explorer, click the address bar,
   type `cmd`, and press Enter.
3. Run these commands in that window. Activation is optional because every
   command below uses the environment's Python explicitly.

```bat
py -m venv .venv
.venv\Scripts\python.exe -m pip install -e ".[dev]"
.venv\Scripts\python.exe -m pytest -q
```

Expected result is `47 passed`. Installation requires internet access. All demo
tests run locally and require no account, market-data subscription, or API key.

On Linux/macOS, create the environment with `python3 -m venv .venv` and replace
`.venv\Scripts\python.exe` below with `.venv/bin/python`.

## Test 1. Clean playback

In the first Command Prompt, create a 20-event synthetic recording and start
the server. The demo command refuses to overwrite an existing file. Reuse an
existing demo or choose another filename when repeating this step.

```bat
.venv\Scripts\python.exe -m feedtest.cli demo demo.jsonl
.venv\Scripts\python.exe -m feedtest.cli replay demo.jsonl
```

Wait for `Replay listening on ws://127.0.0.1:9000`.
Open a second Command Prompt in the same folder and run

```bat
.venv\Scripts\python.exe -m feedtest.cli probe
```

Expected output

```text
PASS {"duplicates": 0, "gaps": 0, "reconnects": 0, "stale": 0, "unique": 20}
```

The server stays running for additional clients. Stop it with Ctrl+C in the
first window before starting the next scenario. Each new connection has its
own replay cursor.

## Test 2. Expose duplicate handling

In the first window, start this server

```bat
.venv\Scripts\python.exe -m feedtest.cli replay demo.jsonl --scenario duplicate --at 5
```

In the second window, run both commands

```bat
.venv\Scripts\python.exe -m feedtest.cli probe --naive
.venv\Scripts\python.exe -m feedtest.cli probe
```

The naive client exits with `FAIL duplicate sequence` and exit code 1.
The resilient client returns `PASS` with `duplicates` equal to 1 and `unique`
equal to 20. Both receive the same injected duplicate of frame 5.

## Test 3. Detect missing data

Restart the first window's server with

```bat
.venv\Scripts\python.exe -m feedtest.cli replay demo.jsonl --scenario gap --at 5
```

Run the probe in the second window

```bat
.venv\Scripts\python.exe -m feedtest.cli probe
```

Expected `FAIL unrecovered gap: expected 5, got 6`. This is a successful
demonstration of detection, but the probe deliberately exits with code 1.
Real recovery requires the venue's snapshot or resend rules. This demo never
reports a gap as recovered just because later messages arrived.

## Test 4. Detect a stale feed

Restart the server with

```bat
.venv\Scripts\python.exe -m feedtest.cli replay demo.jsonl --scenario pause --at 5 --pause 0.5
```

Run both probe commands again. Naive mode returns `FAIL stale feed`.
Resilient mode waits for the feed to resume and returns `PASS` with `stale`
greater than zero and 20 unique events. `stale` counts receive timeouts, so its
exact value depends on scheduling. This client only observes synthetic trades.
A real application should stop dependent decisions while its feed is stale.

## Test 5. Recover from a forced disconnect

Restart the server with

```bat
.venv\Scripts\python.exe -m feedtest.cli replay demo.jsonl --scenario disconnect --at 5
```

Run the naive and resilient probe commands again. Naive mode returns
`FAIL connection lost`. Resilient mode returns `PASS` with `reconnects` equal
to 1 and `unique` equal to 20. It resumes through this tool's explicit
`?start=5` cursor, starting at the sixth frame. Real exchange clients need
their own recovery implementations.

## Test 6. Record and replay raw frames

Restart a clean demo server in the first window. Record it from the second
window using a fresh output filename

```bat
.venv\Scripts\python.exe -m feedtest.cli record ws://127.0.0.1:9000 capture.jsonl --seconds 3
```

Expected `Recorded 20 frames to capture.jsonl`.
Stop the original server and replay the recording

```bat
.venv\Scripts\python.exe -m feedtest.cli replay capture.jsonl
```

Run the probe again in the second window. Expect the clean playback result.

For a public external feed, pass its WebSocket URL to `record`. If it requires
a subscription message, place that message in a UTF-8 file and add
`--subscribe subscription.json`. The recorder sends that message once. Only
record streams whose access and data terms allow your intended use. Keep
credentials and private account messages out of captures you share.

## Integrate your own client

Configure its market-data URL as `ws://127.0.0.1:9000`. Replay your own captured
messages. The server sends raw frames immediately after connection. It does
not interpret subscription requests, perform authentication, implement an
exchange handshake, or emulate REST snapshots. A client requiring those
features needs an adapter before it can use this server unchanged.

`--at` selects a 1-based capture frame, not a venue sequence number. `gap`
omits that frame, and `duplicate` repeats it once. `disconnect` sends it and
then closes with WebSocket code 1012. A reconnect starts at frame zero unless
the client supplies `?start=N`, where N is a zero-based frame offset.
These choices make event content and injection points repeatable without a
random seed. `--speed 2` halves inter-frame delays; `--pause` is in seconds.
Timing uses asyncio and the host scheduler, so this is not a precision latency
benchmark or simulation of TCP packet loss.

The recorder preserves text/binary payloads and monotonic receive-time offsets
as JSONL. Binary frames use base64. Replay preserves capture order, even when
payload timestamps are equal. Captures are loaded into memory before serving;
use modest recordings for this version. Each connection replays independently,
with send backpressure. Probe runs have a 10-second deadline and at most four
connection attempts. The probe supports only the bundled synthetic DEMO
protocol with contiguous sequence numbers. A pause longer than its deadline
will fail. Arbitrary external captures can be recorded and replayed, but cannot
be validated with this demo probe.

## Verification

The test suite checks real local WebSocket connections on automatically
assigned ports. It covers clean and faulty playback, naive-client failures,
gap rejection, binary/text recording, overwrite protection, concurrent client
isolation, repeatable event order, and invalid input.

## Development

`src/feedtest/core.py` owns the capture format and input validation.
`runtime.py` owns recording, replay, and the demo checker. `cli.py` provides
the command-line entry points. `tests/test_feedtest.py` is a runnable example
of embedding the server in your own pytest suite.
