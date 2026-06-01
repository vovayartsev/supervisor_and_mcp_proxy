from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

from mcp import ClientSession, StdioServerParameters, types
from mcp.client.stdio import stdio_client

from .config import McpServerSpec
from .logbuf import RingBuffer


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


async def _pump_fd_to_buf(fd: int, buf: RingBuffer) -> None:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(limit=64 * 1024)
    protocol = asyncio.StreamReaderProtocol(reader)
    transport, _ = await loop.connect_read_pipe(lambda: protocol, os.fdopen(fd, "rb"))
    try:
        while True:
            try:
                line = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as e:
                if e.partial:
                    buf.append("stderr", e.partial.decode("utf-8", errors="replace"))
                return
            except asyncio.LimitOverrunError:
                chunk = await reader.read(8192)
                buf.append("stderr", chunk.decode("utf-8", errors="replace"), truncated=True)
                continue
            if not line:
                return
            buf.append("stderr", line.rstrip(b"\n").decode("utf-8", errors="replace"))
    finally:
        transport.close()


class UpstreamMCP:
    """Manages a single stdio MCP upstream: connect, reconnect, proxied calls."""

    def __init__(self, name: str, spec: McpServerSpec, log_capacity: int) -> None:
        self.name = name
        self.spec = spec
        self.namespace = spec.namespace
        self._log_capacity = log_capacity
        self.log = RingBuffer(log_capacity)
        self.previous_log: RingBuffer | None = None

        self.state = "stopped"  # stopped|starting|running|crashed|backoff
        self.pid: int | None = None  # not tracked; stdio_client hides it
        self.started_at: str | None = None
        self.last_exit_code: int | None = None
        self.last_exit_at: str | None = None
        self.restart_count = 0
        self._restart_window: deque[float] = deque()

        self.session: ClientSession | None = None
        self.tools: list[types.Tool] = []
        self._task: asyncio.Task | None = None
        self._ready = asyncio.Event()
        self._stop_requested = False
        self._call_lock = asyncio.Lock()

    @property
    def tools_proxied(self) -> int:
        return len(self.tools)

    def prefix(self, orig: str) -> str:
        if not self.namespace:
            return orig
        return f"{self.namespace}__{orig}"

    def unprefix(self, name: str) -> str | None:
        if not self.namespace:
            return name
        pfx = f"{self.namespace}__"
        if name.startswith(pfx):
            return name[len(pfx):]
        return None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_requested = False
        self._ready = asyncio.Event()
        self._task = asyncio.create_task(self._run(), name=f"upstream:{self.name}")
        try:
            await asyncio.wait_for(self._ready.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass

    async def stop(self) -> None:
        self._stop_requested = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self.state = "stopped"

    async def _run(self) -> None:
        params = StdioServerParameters(
            command=self.spec.command,
            args=self.spec.args,
            env=self.spec.env or None,
            cwd=self.spec.cwd,
        )
        while True:
            self.state = "starting"
            if len(self.log) > 0:
                self.previous_log = self.log
                self.log = RingBuffer(self._log_capacity)
            read_fd, write_fd = os.pipe()
            errlog = os.fdopen(write_fd, "w", buffering=1)
            pump_task = asyncio.create_task(_pump_fd_to_buf(read_fd, self.log))
            try:
                async with stdio_client(params, errlog=errlog) as (r, w):
                    async with ClientSession(r, w) as session:
                        await session.initialize()
                        self.session = session
                        self.started_at = _iso_now()
                        await self._refresh_tools()
                        self.state = "running"
                        self._ready.set()
                        # idle forever — woken by cancel or stream end
                        while True:
                            await asyncio.sleep(3600)
            except asyncio.CancelledError:
                self.state = "stopped"
                self.session = None
                try:
                    errlog.close()
                except Exception:
                    pass
                pump_task.cancel()
                return
            except Exception as e:
                self.log.append("stderr", f"[supervisor] upstream error: {e}")
                self.last_exit_code = -1
                self.last_exit_at = _iso_now()
                self.state = "crashed"
                self.session = None
            finally:
                try:
                    errlog.close()
                except Exception:
                    pass
                pump_task.cancel()
                try:
                    await pump_task
                except (asyncio.CancelledError, Exception):
                    pass

            if self._stop_requested:
                return

            policy = self.spec.restart_policy
            if policy.mode == "never":
                return
            if policy.mode == "on-failure" and (self.last_exit_code or 0) == 0:
                return

            now = time.monotonic()
            while self._restart_window and now - self._restart_window[0] > policy.window_seconds:
                self._restart_window.popleft()
            self._restart_window.append(now)
            self.restart_count += 1
            if len(self._restart_window) > policy.max_restarts:
                self.state = "backoff"
                return
            await asyncio.sleep(policy.backoff_seconds)

    async def _refresh_tools(self) -> None:
        try:
            res = await self.session.list_tools()
            self.tools = list(res.tools)
        except Exception as e:
            self.log.append("stderr", f"[supervisor] list_tools failed: {e}")
            self.tools = []

    async def call_tool(self, orig_name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        if self.state != "running" or not self.session:
            raise RuntimeError(f"upstream {self.name} not available, state={self.state}")
        async with self._call_lock:
            return await self.session.call_tool(orig_name, arguments)

    def status(self) -> dict:
        return {
            "kind": "mcp_server",
            "state": self.state,
            "pid": self.pid,
            "started_at": self.started_at,
            "uptime_seconds": self._uptime(),
            "restart_count": self.restart_count,
            "last_exit_code": self.last_exit_code,
            "last_exit_at": self.last_exit_at,
            "tools_proxied": self.tools_proxied,
        }

    def _uptime(self) -> float | None:
        if self.state != "running" or not self.started_at:
            return None
        started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - started).total_seconds()
