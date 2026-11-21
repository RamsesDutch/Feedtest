# Dashboard walkthrough

Launch `python -m feedtest dashboard` from an installed project environment and
open http://127.0.0.1:8765. Use a new extracted folder when upgrading releases.
The existing command-line tools remain available for automation.

## Acceptance walkthrough

1. Leave the demo capture and all five checks selected. Click Run checks.
   Expect five rows marked passed and a final 5 / 5 count.
2. Read the gap and pause rows. Both should show an expected rejection.
   A passed check confirms the expected client behavior; it does not imply
   that a damaged stream became usable.
3. Click Export JSON. Open the downloaded file and check the capture,
   configuration, expected behavior, observed results, and passed count.
4. Select Disconnect and resubscribe, then Start external replay. Copy the
   displayed URL into a configurable Kraken v2 client. Each logical client
   should use its own session name. Press Stop operations to close the replay.
5. Start a pause check with a 5-second pause and immediately press Stop
   operations. Confirm the activity log records cancellation. Run clean
   playback again to confirm the dashboard remains usable.
6. Import a valid Kraken JSONL capture through the file picker. It should
   appear in the capture list after checksum validation. Invalid imports
   should produce a visible error and leave the list unchanged.
7. Optionally expand Record from Kraken's public feed, use BTC/USD and 10
   seconds, and click Record public feed. A successful validated recording
   appears in the list. Network or exchange errors appear in Activity.
8. Stop with Ctrl+C, restart in the same directory, and expand Previous
   reports. Completed reports and captures should remain available.

## Behavior and boundaries

One background operation runs at a time. A second request returns HTTP 409.
An external replay can remain active while checks run on a separate ephemeral
port. Stop operations cancels the active job and closes the external replay.
Process shutdown closes these resources as well. Cancellation does not create
a completed report. A recording is validated before registration; incomplete
recordings are deleted on cancellation or failure.

Capture imports and dashboard recordings are limited to 8 MiB. Larger files
can still be used through the command-line workflow, subject to available
memory. JSONL captures are stored under `captures/` and completed reports
under `reports/` in the data directory. Capture content is retained locally.
No telemetry, remote account, or external page assets are used.

Dashboard reports show correctness checks from a separately implemented
consumer. They do not measure production latency. Checks against arbitrary
recordings can fail because a chosen omission is undetectable, the pause is
too short, or playback exceeds the 30-second client deadline. Choose a short
capture and a fault frame that demonstrates the behavior under investigation.
The synthetic fixture is designed for all five checks.

The dashboard is for use on the same computer. It binds to loopback and uses
a per-process request token to protect its API from unrelated browser pages.
It is not a hosted multi-user application. Public recording always uses the
configured public Kraken endpoint, not a browser-supplied destination URL.

## Browser tests

The optional Playwright suite exercises the rendered dashboard, suite results,
report downloads, external replay controls, and a mobile viewport. Run

```sh
python -m pip install -e ".[dev,ui]"
python -m playwright install chromium
python -m pytest ui_tests -q
```

GitHub Actions has a separate browser job that installs Chromium and its
Linux dependencies. This is separate from the default local pytest suite so
end users do not need browser automation packages to use FeedTest.
