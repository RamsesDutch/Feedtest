# Contributing

Create an issue describing a reproducible client problem, or submit a focused
pull request with a fixture and regression test. Include the documented venue
rules when changing an adapter. Use synthetic fixtures or data you can legally
redistribute. Remove credentials and private account messages.

Use Python 3.11 or newer, then run from the repository root

```sh
python -m pip install -e ".[dev]"
python -m ruff check .
python -m pytest -q
python -m build
```

The CI matrix runs on Ubuntu and Windows with Python 3.11, 3.12, and 3.13.
Tests use local sockets and ephemeral ports. No live market connection is
required. Keep tests independent of download availability and exchange uptime.
Match fault-specific errors so an unrelated timeout cannot pass a gap test.

For bug reports, include Python/OS versions, the command, expected and actual
behavior, and a minimal shareable capture or fixture generator. For a timing
failure, include the timeout values. Wall-clock scheduling is not deterministic.

Before publishing a release, check the distribution name's availability, build
from a clean checkout, inspect wheel/sdist contents, test the wheel in a new
environment, and confirm the GitHub Actions matrix passes. Publication is manual.
