"""Local dashboard, task lifecycle, and persisted test evidence."""

import asyncio
import json
import math
import secrets
import uuid
from collections import deque
from contextlib import suppress
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path

from aiohttp import web
from websockets.asyncio.server import serve

from .core import SCENARIOS, load_capture
from .example_client import run as consume
from .kraken import KRAKEN_URL, kraken_handler, make_kraken_demo, prepare_capture, subscription
from .runtime import record

CONTROLLER = web.AppKey("controller", object)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Dashboard:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.captures = self.directory / "captures"
        self.reports = self.directory / "reports"
        self.captures.mkdir(parents=True, exist_ok=True)
        self.reports.mkdir(exist_ok=True)
        self.token = secrets.token_urlsafe(32)
        self.task = None
        self.operation = None
        self.server = None
        self.endpoint = None
        self.events = deque(maxlen=100)
        self.results = []
        self.last_report = None
        demo = self.captures / "demo.jsonl"
        if not demo.exists():
            make_kraken_demo(demo)
        self.note("Ready. The bundled capture is synthetic Kraken-format data.")

    def note(self, message):
        self.events.append({"time": now(), "message": message})

    def path(self, name):
        if (
            not isinstance(name, str)
            or not name
            or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in name)
        ):
            raise ValueError("invalid capture identifier")
        path = self.captures / f"{name}.jsonl"
        if not path.is_file() or path.is_symlink():
            raise ValueError("capture not found")
        return path

    def capture_info(self, path):
        if path.stat().st_size > 8 * 1024 * 1024:
            raise ValueError("capture exceeds dashboard limit of 8 MiB")
        frames = load_capture(path)
        # Determine the single symbol without trusting a browser-supplied value.
        symbol = None
        for frame in frames:
            message = json.loads(frame.payload)
            if isinstance(message, dict) and message.get("channel") == "book":
                data = message.get("data")
                if not isinstance(data, list) or not data or not isinstance(data[0], dict):
                    raise ValueError("invalid Kraken book data")
                symbol = data[0].get("symbol")
                break
        if not isinstance(symbol, str) or not symbol or len(symbol) > 32:
            raise ValueError("capture has no supported Kraken book symbol")
        selected = prepare_capture(frames, symbol)
        return {
            "id": path.stem,
            "name": path.name,
            "symbol": symbol,
            "frames": len(selected),
            "bytes": path.stat().st_size,
            "source": "Synthetic demo" if path.stem == "demo" else "Imported or recorded",
        }

    def state(self):
        captures = []
        for path in sorted(self.captures.glob("*.jsonl")):
            try:
                captures.append(self.capture_info(path))
            except (ValueError, KeyError, TypeError, OSError):
                continue
        history = []
        for path in sorted(self.reports.glob("*.json"), reverse=True)[:20]:
            try:
                report = json.loads(path.read_text(encoding="utf-8"))
                history.append(
                    {
                        "id": path.stem,
                        "created": report["created"],
                        "passed": report["passed"],
                        "total": len(report["results"]),
                    }
                )
            except (ValueError, KeyError, OSError):
                continue
        return {
            "operation": self.operation,
            "endpoint": self.endpoint,
            "captures": captures,
            "results": self.results,
            "events": list(self.events),
            "history": history,
            "last_report": self.last_report,
        }

    def options(self, body):
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        path = self.path(body.get("capture", "demo"))
        info = self.capture_info(path)
        scenario = body.get("scenario", "clean")
        if scenario not in (*SCENARIOS, "suite"):
            raise ValueError("unknown scenario")
        at = body.get("at", 3)
        if type(at) is not int or not 2 <= at <= info["frames"]:
            raise ValueError("fault frame must select an update after the snapshot")
        pause, speed = body.get("pause", 3), body.get("speed", 1)
        if any(
            type(value) not in (int, float) or not math.isfinite(value) for value in (pause, speed)
        ):
            raise ValueError("pause and speed must be finite numbers")
        if not 0.1 <= pause <= 10 or not 0.25 <= speed <= 20:
            raise ValueError("pause must be 0.1–10 seconds; speed must be 0.25–20")
        return path, info, {"scenario": scenario, "at": at, "pause": pause, "speed": speed}

    def start_task(self, label, coroutine):
        if self.task is not None and not self.task.done():
            coroutine.close()
            raise web.HTTPConflict(text="Another operation is running. Stop it first.")
        self.operation = label
        self.task = asyncio.create_task(self.work(coroutine))

    async def work(self, coroutine):
        try:
            await coroutine
        except asyncio.CancelledError:
            self.note("Operation cancelled.")
            raise
        except Exception as error:
            self.note(f"Operation failed: {error or type(error).__name__}")
        finally:
            self.operation = None

    async def cancel(self):
        task = self.task
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def stop_replay(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
            self.endpoint = None
            self.note("Replay stopped; local port released.")

    async def start_replay(self, path, info, options):
        if options["scenario"] == "suite":
            raise ValueError("Choose one scenario to start an external replay.")
        # Validate before replacing a working server.
        handler = kraken_handler(load_capture(path), info["symbol"], **options)
        await self.stop_replay()
        self.server = await serve(handler, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.endpoint = f"ws://127.0.0.1:{port}?session=external-{uuid.uuid4().hex[:8]}"
        self.note(f"Replay started for external clients: {self.endpoint}")

    async def run_checks(self, path, info, options):
        self.results = []
        self.last_report = None
        scenarios = SCENARIOS if options["scenario"] == "suite" else [options["scenario"]]
        self.note(f"Testing {info['name']} with the independent consumer.")
        for scenario in scenarios:
            config = {**options, "scenario": scenario}
            expected = {"gap": "checksum mismatch", "pause": "stale feed"}.get(scenario)
            result = {
                "scenario": scenario,
                "status": "failed",
                "expected": expected or "validated book at replay completion",
            }
            try:
                handler = kraken_handler(load_capture(path), info["symbol"], **config)
                async with serve(handler, "127.0.0.1", 0) as server:
                    port = server.sockets[0].getsockname()[1]
                    summary = await consume(
                        f"ws://127.0.0.1:{port}?session=check",
                        info["symbol"],
                        stale_after=2,
                        timeout=30,
                    )
                result["summary"] = summary
                result["status"] = "passed" if expected is None else "failed"
                result["detail"] = (
                    "Book validated" if expected is None else "Injected fault was not detected"
                )
            except Exception as error:
                message = str(error) or type(error).__name__
                result["detail"] = message
                if expected and isinstance(error, ValueError) and expected in message:
                    result["status"] = "passed"
                    result["detail"] = f"Expected rejection observed: {message}"
            self.results.append(result)
            self.note(f"{scenario}: {result['status']} — {result['detail']}")
        identifier = (
            datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S") + "-" + uuid.uuid4().hex[:8]
        )
        report = {
            "version": "0.3.1",
            "created": now(),
            "capture": info,
            "configuration": options,
            "results": self.results,
            "passed": sum(row["status"] == "passed" for row in self.results),
        }
        target = self.reports / f"{identifier}.json"
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps(report, indent=2), encoding="utf-8")
        temporary.replace(target)
        self.last_report = identifier
        self.note(f"Report saved. {report['passed']}/{len(self.results)} checks passed.")

    async def record_public(self, seconds, symbol):
        identifier = "recording-" + uuid.uuid4().hex[:12]
        partial = self.captures / f"{identifier}.part"
        final = self.captures / f"{identifier}.jsonl"
        self.note(f"Recording public Kraken book for {symbol}, up to {seconds} seconds.")
        try:
            async with asyncio.timeout(seconds + 15):
                await record(KRAKEN_URL, partial, seconds, subscription(symbol))
            self.capture_info(partial)
            partial.replace(final)
            self.note(f"Recording validated and saved as {final.name}.")
        finally:
            partial.unlink(missing_ok=True)


def create_app(directory):
    controller = Dashboard(directory)

    @web.middleware
    async def protect(request, handler):
        if request.url.host not in ("127.0.0.1", "localhost"):
            raise web.HTTPForbidden(text="Local host required")
        if request.path.startswith("/api/"):
            if not secrets.compare_digest(
                request.headers.get("X-FeedTest-Token", ""), controller.token
            ):
                raise web.HTTPForbidden(text="Reload the dashboard to authorize this request")
        try:
            response = await handler(request)
        except (ValueError, KeyError, TypeError, OSError) as error:
            return web.json_response({"error": str(error)}, status=400)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    app = web.Application(middlewares=[protect], client_max_size=8 * 1024 * 1024)
    app[CONTROLLER] = controller

    async def index(request):
        html = files("feedtest").joinpath("web/index.html").read_text(encoding="utf-8")
        return web.Response(
            text=html.replace("__TOKEN__", controller.token), content_type="text/html"
        )

    async def state(request):
        return web.json_response(controller.state())

    async def run(request):
        path, info, options = controller.options(await request.json())
        controller.start_task("Running checks", controller.run_checks(path, info, options))
        return web.json_response({"started": True}, status=202)

    async def replay(request):
        path, info, options = controller.options(await request.json())
        controller.start_task("Starting replay", controller.start_replay(path, info, options))
        return web.json_response({"started": True}, status=202)

    async def stop(request):
        await controller.cancel()
        await controller.stop_replay()
        return web.json_response({"stopped": True})

    async def upload(request):
        payload = await request.read()
        identifier = "import-" + uuid.uuid4().hex[:12]
        path = controller.captures / f"{identifier}.jsonl"
        try:
            path.write_bytes(payload)
            info = controller.capture_info(path)
        except Exception:
            path.unlink(missing_ok=True)
            raise
        controller.note(f"Imported and validated {info['frames']} book frames.")
        return web.json_response(info, status=201)

    async def record_feed(request):
        body = await request.json()
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        seconds = body.get("seconds", 10)
        symbol = body.get("symbol", "BTC/USD")
        if type(seconds) is not int or not 1 <= seconds <= 30:
            raise ValueError("recording duration must be 1–30 whole seconds")
        if not isinstance(symbol, str) or not 3 <= len(symbol) <= 32:
            raise ValueError("invalid Kraken symbol")
        controller.start_task("Recording public feed", controller.record_public(seconds, symbol))
        return web.json_response({"started": True}, status=202)

    async def download(request):
        identifier = request.match_info["id"]
        if any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-" for c in identifier):
            raise web.HTTPNotFound()
        path = controller.reports / f"{identifier}.json"
        if not path.is_file() or path.is_symlink():
            raise web.HTTPNotFound()
        return web.Response(
            body=path.read_bytes(),
            content_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{identifier}.json"'},
        )

    async def cleanup(application):
        await controller.cancel()
        await controller.stop_replay()

    app.on_cleanup.append(cleanup)
    app.add_routes(
        [
            web.get("/", index),
            web.get("/api/state", state),
            web.post("/api/run", run),
            web.post("/api/replay", replay),
            web.post("/api/stop", stop),
            web.post("/api/upload", upload),
            web.post("/api/record", record_feed),
            web.get("/api/reports/{id}", download),
        ]
    )
    return app


def launch(directory, port=8765, open_browser=False):
    def ready(message):
        print(f"Open http://127.0.0.1:{port} — Ctrl+C stops FeedTest.", flush=True)
        if open_browser:
            import webbrowser

            webbrowser.open(f"http://127.0.0.1:{port}")

    web.run_app(create_app(directory), host="127.0.0.1", port=port, print=ready)
