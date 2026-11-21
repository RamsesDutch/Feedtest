import sys

import pytest

from feedtest import cli
from feedtest.kraken import KRAKEN_URL, make_kraken_demo


def test_record_kraken_uses_subscription_and_validates(monkeypatch, tmp_path, capsys):
    path = tmp_path / "public.jsonl"
    calls = []

    async def recording(url, output, seconds, subscribe):
        calls.append((url, seconds, subscribe))
        make_kraken_demo(output)

    monkeypatch.setattr(cli, "record", recording)
    monkeypatch.setattr(sys, "argv", ["feedtest", "record-kraken", str(path)])
    cli.main()
    assert calls[0][0] == KRAKEN_URL
    assert '"channel": "book"' in calls[0][2]
    assert "PASS captured book checksums validated" in capsys.readouterr().out


def test_validate_command_rejects_damaged_capture(monkeypatch, tmp_path):
    path = tmp_path / "broken.jsonl"
    path.write_text("{}\n")
    monkeypatch.setattr(sys, "argv", ["feedtest", "validate", str(path)])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 1


def test_dashboard_launch_options(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr("feedtest.dashboard.launch", lambda *args: calls.append(args))
    monkeypatch.setattr(
        sys,
        "argv",
        ["feedtest", "dashboard", "--port", "8766", "--data-dir", str(tmp_path), "--open-browser"],
    )
    cli.main()
    assert calls == [(str(tmp_path), 8766, True)]
