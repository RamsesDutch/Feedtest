import asyncio
import json
from contextlib import asynccontextmanager
from html.parser import HTMLParser
from importlib.resources import files

import pytest
from aiohttp.test_utils import TestClient, TestServer

from feedtest.dashboard import CONTROLLER, Dashboard, create_app
from feedtest.example_client import run as consume
from feedtest.kraken import make_kraken_demo


def test_dashboard_scenario_options_are_real_option_elements():
    class Options(HTMLParser):
        def __init__(self):
            super().__init__()
            self.in_scenarios = False
            self.values = []

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "select":
                self.in_scenarios = attrs.get("id") == "scenario"
            if self.in_scenarios and tag == "option":
                self.values.append(attrs.get("value"))

        def handle_endtag(self, tag):
            if tag == "select":
                self.in_scenarios = False

    parser = Options()
    parser.feed(files("feedtest").joinpath("web/index.html").read_text(encoding="utf-8"))
    assert parser.values == ["suite", "clean", "duplicate", "gap", "pause", "disconnect"]


@asynccontextmanager
async def dashboard(directory):
    app = create_app(directory)
    async with TestClient(TestServer(app, host="127.0.0.1")) as client:
        controller = app[CONTROLLER]
        client.session.headers["X-FeedTest-Token"] = controller.token
        yield client, controller


def test_full_suite_and_report_download(tmp_path):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            home = await client.get("/")
            assert home.status == 200
            assert controller.token in await home.text()
            response = await client.post("/api/run", json={"scenario": "suite"})
            assert response.status == 202
            await controller.task
            state = await (await client.get("/api/state")).json()
            assert len(state["results"]) == 5
            assert all(row["status"] == "passed" for row in state["results"])
            report = await client.get("/api/reports/" + state["last_report"])
            assert report.status == 200
            assert (await report.json())["passed"] == 5
            assert len(state["history"]) == 1
        reloaded = Dashboard(tmp_path)
        assert len(reloaded.state()["history"]) == 1

    asyncio.run(run())


def test_external_replay_stop_and_restart(tmp_path):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            for _ in range(2):
                response = await client.post("/api/replay", json={"scenario": "disconnect"})
                assert response.status == 202
                await controller.task
                endpoint = controller.endpoint
                result = await consume(endpoint)
                assert result["reconnects"] == 1
                assert result["final_connection_frames"] == 8
                response = await client.post("/api/stop", json={})
                assert response.status == 200
                assert controller.server is None
                assert controller.endpoint is None

    asyncio.run(run())


def test_conflicting_task_and_cancellation(tmp_path):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            response = await client.post("/api/run", json={"scenario": "pause", "pause": 5})
            assert response.status == 202
            response = await client.post("/api/run", json={"scenario": "clean"})
            assert response.status == 409
            response = await client.post("/api/stop", json={})
            assert response.status == 200
            assert controller.operation is None
            response = await client.post("/api/run", json={"scenario": "clean"})
            assert response.status == 202
            await controller.task
            assert controller.results[0]["status"] == "passed"

    asyncio.run(run())


@pytest.mark.parametrize(
    "body",
    [
        [],
        {"capture": "../secret"},
        {"at": True},
        {"pause": -1},
        {"speed": "fast"},
        {"scenario": "anything"},
    ],
)
def test_invalid_requests(tmp_path, body):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            response = await client.post("/api/run", json=body)
            assert response.status == 400
            assert controller.operation is None

    asyncio.run(run())


def test_upload_validation_and_bad_upload_cleanup(tmp_path):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            fixture = tmp_path / "authored.jsonl"
            make_kraken_demo(fixture)
            response = await client.post("/api/upload", data=fixture.read_bytes())
            assert response.status == 201
            assert (await response.json())["frames"] == 8
            before = list(controller.captures.glob("*.jsonl"))
            for payload in (
                b"broken",
                b"{}\n",
                b'{"offset_ns":0,"binary":false,"payload":"{\\"channel\\":\\"book\\",\\"data\\":[]}"}\n',
            ):
                response = await client.post("/api/upload", data=payload)
                assert response.status == 400
            assert list(controller.captures.glob("*.jsonl")) == before

    asyncio.run(run())


def test_token_and_host_protection(tmp_path):
    async def run():
        async with dashboard(tmp_path) as (client, controller):
            response = await client.get("/api/state", headers={"X-FeedTest-Token": "wrong"})
            assert response.status == 403
            response = await client.get("/", headers={"Host": "unexpected.example"})
            assert response.status == 403
            response = await client.get("/api/reports/not-found")
            assert response.status == 404

    asyncio.run(run())


def test_recording_cancellation_removes_partial_file(tmp_path, monkeypatch):
    async def fake_record(url, output, seconds, subscription):
        output.write_text("partial")
        await asyncio.sleep(30)

    monkeypatch.setattr("feedtest.dashboard.record", fake_record)

    async def run():
        async with dashboard(tmp_path) as (client, controller):
            response = await client.post("/api/record", json={"seconds": 10})
            assert response.status == 202
            await asyncio.sleep(0)
            await client.post("/api/stop", json={})
            assert not list(controller.captures.glob("*.part"))
            assert len(list(controller.captures.glob("*.jsonl"))) == 1

    asyncio.run(run())


def test_recording_success_is_validated_before_registration(tmp_path, monkeypatch):
    async def fake_record(url, output, seconds, subscription):
        assert json.loads(subscription)["params"]["snapshot"] is True
        make_kraken_demo(output)

    monkeypatch.setattr("feedtest.dashboard.record", fake_record)

    async def run():
        async with dashboard(tmp_path) as (client, controller):
            response = await client.post("/api/record", json={"seconds": 1})
            assert response.status == 202
            await controller.task
            assert len(controller.state()["captures"]) == 2
            assert not list(controller.captures.glob("*.part"))

    asyncio.run(run())
