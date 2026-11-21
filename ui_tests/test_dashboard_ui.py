"""Optional actual-browser checks. See docs/dashboard.md."""

import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright


def test_dashboard_in_browser(tmp_path):
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "feedtest",
            "dashboard",
            "--port",
            str(port),
            "--data-dir",
            str(tmp_path / "data"),
        ],
        stdout=subprocess.DEVNULL,
    )
    try:
        url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + 15
        while True:
            try:
                with urllib.request.urlopen(url, timeout=1) as response:
                    assert response.status == 200
                break
            except urllib.error.URLError:
                if server.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("dashboard failed to start")
                time.sleep(0.1)
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(viewport={"width": 1440, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(url)
            page.wait_for_selector("#capture option", state="attached")
            assert page.locator("#scenario option").count() == 6
            page.select_option("#scenario", "clean")
            assert page.locator("#scenario").input_value() == "clean"
            page.select_option("#scenario", "suite")
            page.click("#run")
            page.wait_for_function(
                "document.querySelector('#passed').textContent === '5 / 5'", timeout=20000
            )
            page.wait_for_function("document.querySelector('#activity').textContent === 'Ready'")
            assert page.locator("#results tr").count() == 5
            with page.expect_download() as download:
                page.click("#download")
            report_path = download.value.path()
            assert json.loads(report_path.read_text())["passed"] == 5
            page.select_option("#scenario", "disconnect")
            page.click("#replay")
            page.wait_for_function(
                "document.querySelector('#replay-state').textContent === 'Listening'"
            )
            assert "ws://127.0.0.1:" in page.locator("#endpoint").inner_text()
            page.click("#stop")
            page.wait_for_function(
                "document.querySelector('#replay-state').textContent === 'Stopped'"
            )
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
            assert not errors
            browser.close()
    finally:
        server.terminate()
        server.wait(timeout=10)
