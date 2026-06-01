from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Literal, TypedDict

MAX_LINE_BYTES = 8 * 1024


class LogLine(TypedDict, total=False):
    time: str
    stream: Literal["stdout", "stderr"]
    message: str
    truncated: bool


class RingBuffer:
    def __init__(self, capacity: int) -> None:
        self._dq: deque[LogLine] = deque(maxlen=capacity)

    def append(self, stream: str, message: str, truncated: bool = False) -> None:
        line: LogLine = {
            "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "stream": stream,  # type: ignore[typeddict-item]
            "message": message,
            "truncated": truncated,
        }
        if not truncated:
            line.pop("truncated", None)
        self._dq.append(line)

    def tail(self, n: int, stream: str = "all") -> list[LogLine]:
        items = list(self._dq)
        if stream != "all":
            items = [x for x in items if x["stream"] == stream]
        return items[-n:] if n > 0 else items

    def __len__(self) -> int:
        return len(self._dq)


async def pump_stream(
    reader: asyncio.StreamReader,
    buf: RingBuffer,
    stream: Literal["stdout", "stderr"],
) -> None:
    while True:
        try:
            chunk = await reader.readuntil(b"\n")
        except asyncio.IncompleteReadError as e:
            chunk = e.partial
            if not chunk:
                return
        except asyncio.LimitOverrunError:
            chunk = await reader.read(MAX_LINE_BYTES)
            _truncate_remaining_line(reader)
            text = chunk.decode("utf-8", errors="replace").rstrip("\n")
            buf.append(stream, text, truncated=True)
            continue
        if not chunk:
            return
        raw = chunk.rstrip(b"\n")
        truncated = False
        if len(raw) > MAX_LINE_BYTES:
            raw = raw[:MAX_LINE_BYTES]
            truncated = True
        buf.append(stream, raw.decode("utf-8", errors="replace"), truncated)


def _truncate_remaining_line(reader: asyncio.StreamReader) -> None:
    # best-effort discard until newline
    pass
