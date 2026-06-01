from __future__ import annotations

import asyncio
import os
import shlex
import signal
from collections import deque
from datetime import datetime, timezone
from typing import Literal

from .config import RestartPolicy
from .logbuf import RingBuffer, pump_stream

State = Literal["stopped", "starting", "running", "crashed", "backoff"]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_argv(command: str, args: list[str]) -> list[str]:
    if args:
        return [command, *args]
    return shlex.split(command)


class ManagedProcess:
    """Single supervised process with restart policy + crash-loop backoff."""

    def __init__(
        self,
        name: str,
        command: str,
        args: list[str],
        env: dict[str, str],
        cwd: str | None,
        restart_policy: RestartPolicy,
        log_capacity: int,
        on_state_change=None,
    ) -> None:
        self.name = name
        self.command = command
        self.args = args
        self.env = env
        self.cwd = cwd
        self.policy = restart_policy
        self._log_capacity = log_capacity
        self.log = RingBuffer(log_capacity)
        self.previous_log: RingBuffer | None = None

        self.state: State = "stopped"
        self.pid: int | None = None
        self.started_at: str | None = None
        self.last_exit_code: int | None = None
        self.last_exit_at: str | None = None
        self.restart_count: int = 0
        self._restart_window: deque[float] = deque()

        self._proc: asyncio.subprocess.Process | None = None
        self._supervisor_task: asyncio.Task | None = None
        self._stop_requested = False
        self._exited = asyncio.Event()
        self._first_spawn_done = asyncio.Event()
        self._on_state_change = on_state_change

    # --- public API ---

    async def start(self) -> None:
        if self.state in ("running", "starting"):
            return
        self._stop_requested = False
        self.restart_count = 0
        self._restart_window.clear()
        self._first_spawn_done = asyncio.Event()
        self._supervisor_task = asyncio.create_task(self._supervise(), name=f"sup:{self.name}")
        await self._first_spawn_done.wait()

    async def stop(self, sig: str = "TERM", grace: float = 10.0) -> int | None:
        self._stop_requested = True
        if self._proc and self._proc.returncode is None:
            try:
                self._proc.send_signal(getattr(signal, f"SIG{sig}", signal.SIGTERM))
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self._exited.wait(), timeout=grace)
            except asyncio.TimeoutError:
                if self._proc and self._proc.returncode is None:
                    try:
                        self._proc.kill()
                    except ProcessLookupError:
                        pass
                    await self._exited.wait()
        if self._supervisor_task:
            try:
                await asyncio.wait_for(self._supervisor_task, timeout=grace)
            except asyncio.TimeoutError:
                self._supervisor_task.cancel()
        self._set_state("stopped")
        return self.last_exit_code

    def status(self) -> dict:
        out = {
            "state": self.state,
            "pid": self.pid,
            "started_at": self.started_at,
            "uptime_seconds": self._uptime(),
            "restart_count": self.restart_count,
            "last_exit_code": self.last_exit_code,
            "last_exit_at": self.last_exit_at,
        }
        return out

    # --- internals ---

    def _uptime(self) -> float | None:
        if self.state != "running" or not self.started_at:
            return None
        started = datetime.fromisoformat(self.started_at.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - started).total_seconds()

    def _set_state(self, s: State) -> None:
        self.state = s
        if self._on_state_change:
            try:
                self._on_state_change(self.name, s)
            except Exception:
                pass

    async def _spawn(self) -> None:
        self._set_state("starting")
        argv = _build_argv(self.command, self.args)
        env = {**os.environ, **self.env}
        self._exited = asyncio.Event()
        if len(self.log) > 0:
            self.previous_log = self.log
            self.log = RingBuffer(self._log_capacity)
        self._proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=self.cwd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        self.pid = self._proc.pid
        self.started_at = _iso_now()
        self._set_state("running")
        asyncio.create_task(pump_stream(self._proc.stdout, self.log, "stdout"))
        asyncio.create_task(pump_stream(self._proc.stderr, self.log, "stderr"))

    async def _supervise(self) -> None:
        while True:
            try:
                await self._spawn()
            except Exception as e:
                self.log.append("stderr", f"[supervisor] spawn failed: {e}")
                self.last_exit_code = -1
                self.last_exit_at = _iso_now()
                self._set_state("crashed")
                self._first_spawn_done.set()
            else:
                self._first_spawn_done.set()
                rc = await self._proc.wait()
                self.last_exit_code = rc
                self.last_exit_at = _iso_now()
                self.pid = None
                self._exited.set()
                self._set_state("crashed" if rc != 0 else "stopped")

            if self._stop_requested:
                return

            mode = self.policy.mode
            if mode == "never":
                return
            if mode == "on-failure" and (self.last_exit_code or 0) == 0:
                return

            # restart bookkeeping
            now = asyncio.get_event_loop().time()
            window = self.policy.window_seconds
            while self._restart_window and now - self._restart_window[0] > window:
                self._restart_window.popleft()
            self._restart_window.append(now)
            self.restart_count += 1

            if len(self._restart_window) > self.policy.max_restarts:
                self._set_state("backoff")
                return

            await asyncio.sleep(self.policy.backoff_seconds)
            if self._stop_requested:
                return
