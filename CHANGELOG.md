# Changelog

## 0.3.1

- Fixed a malformed HTML tag that could hide the standalone clean-playback
  option in the scenario selector. Added a regression check for all options.
- Formatted dashboard HTML, CSS, and JavaScript for source review.
- Expanded gitignore coverage for local data and development artifacts.

## 0.3.0

- Added a browser dashboard with capture import, public recording, scenario
  selection, test execution, external replay, cancellation, and JSON exports.
- Persisted completed reports and captures in a configurable local data folder.
- Packaged the independent consumer so installed wheels support dashboard tests.
- Added dashboard lifecycle, authorization, validation, and report tests.
- Added an optional browser test suite and GitHub Actions browser-check job.

## 0.2.0

- Added a Kraken Spot v2 book adapter for one symbol at depth 10.
- Added Decimal-preserving CRC validation and capture validation command.
- Added an independent example client that rebuilds from snapshot on reconnect.
- Added synthetic Kraken fixtures and offline integration tests.
- Added a context manager for embedding replay in Python tests.
- Added GitHub Actions, lint configuration, build metadata, and MIT licensing.
- Preserved existing raw replay commands and the original DEMO protocol.

## 0.1.0

- Raw text/binary capture, replay, and deterministic frame-based faults.
- DEMO client with naive and resilient modes.
- Corrected a test that assumed normal delivery always took less than 30 ms.
