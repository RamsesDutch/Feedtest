"""Transport independent capture format and deterministic scenario planning."""

import base64
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Frame:
    offset_ns: int
    payload: str | bytes


def save_frame(stream, frame: Frame):
    binary = isinstance(frame.payload, bytes)
    payload = base64.b64encode(frame.payload).decode() if binary else frame.payload
    stream.write(
        json.dumps({"offset_ns": frame.offset_ns, "binary": binary, "payload": payload}) + "\n"
    )
    stream.flush()


def load_capture(path):
    frames = []
    previous = -1
    with Path(path).open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            try:
                row = json.loads(line)
                offset = row["offset_ns"]
                if type(offset) is not int or offset < 0 or offset < previous:
                    raise ValueError("offset_ns must be nonnegative and monotonic")
                if type(row["binary"]) is not bool or not isinstance(row["payload"], str):
                    raise ValueError("invalid payload or binary flag")
                payload = row["payload"]
                if row["binary"]:
                    payload = base64.b64decode(payload, validate=True)
                frames.append(Frame(offset, payload))
                previous = offset
            except (ValueError, KeyError, TypeError) as error:
                raise ValueError(f"Invalid capture line {number}: {error}") from error
    if not frames:
        raise ValueError("capture is empty")
    return frames


SCENARIOS = ("clean", "duplicate", "gap", "pause", "disconnect")


def validate_scenario(frames, scenario, at, pause, speed):
    import math

    if scenario not in SCENARIOS:
        raise ValueError("unknown scenario")
    if not math.isfinite(speed) or speed <= 0:
        raise ValueError("speed must be finite and greater than zero")
    if not math.isfinite(pause) or pause < 0:
        raise ValueError("pause must be finite and nonnegative")
    if scenario != "clean" and not 1 <= at <= len(frames):
        raise ValueError("at must select a frame in the capture (1-based)")
    if scenario == "disconnect" and at == len(frames):
        raise ValueError("disconnect requires at least one subsequent frame")


def make_demo(path, count=20):
    if count < 2:
        raise ValueError("count must be at least 2")
    with Path(path).open("x", encoding="utf-8") as stream:
        for index in range(count):
            payload = json.dumps(
                {
                    "type": "trade",
                    "symbol": "DEMO",
                    "sequence": index + 1,
                    "price": 100 + index,
                    "size": 1,
                }
            )
            save_frame(stream, Frame(index * 50_000_000, payload))
