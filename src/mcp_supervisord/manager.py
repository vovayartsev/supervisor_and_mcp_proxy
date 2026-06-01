from __future__ import annotations

import asyncio
import os
import signal
import time
from collections import OrderedDict
from datetime import datetime, timezone

from .config import NamedToolSpec, RestartPolicy, SupervisorConfig
from .logbuf import RingBuffer, pump_stream
from .process import ManagedProcess, _iso_now

BASH_PID_LRU = 100


class BashTracked:
    __slots__ = ("proc", "log", "started_at", "exit_code", "exited_at")

    def __init__(self, proc: asyncio.subprocess.Process, log: RingBuffer) -> None:
        self.proc = proc
        self.log = log
        self.started_at = time.monotonic()
        self.exit_code: int | None = None
        self.exited_at: float | None = None


class ProcessManager:
    def __init__(self, config: SupervisorConfig) -> None:
        self.config = config
        self.named: dict[str, ManagedProcess] = {}
        self.bash_pids: OrderedDict[int, BashTracked] = OrderedDict()
        self._init_named()

    def _init_named(self) -> None:
        for name, spec in self.config.named_tools.items():
            self.named[name] = self._build_managed(name, spec)

    def _build_managed(self, name: str, spec: NamedToolSpec) -> ManagedProcess:
        return ManagedProcess(
            name=name,
            command=spec.command,
            args=spec.args,
            env=spec.env,
            cwd=spec.cwd,
            restart_policy=spec.restart_policy,
            log_capacity=self.config.log_buffer,
        )

    async def autostart(self) -> None:
        for name, spec in self.config.named_tools.items():
            if spec.autostart:
                await self.named[name].start()

    async def start(self, name: str) -> dict:
        if name not in self.named:
            raise KeyError(f"unknown tool {name!r}")
        mp = self.named[name]
        if mp.state == "running":
            return {"pid": mp.pid, "state": mp.state, "error": "already running"}
        await mp.start()
        return {"pid": mp.pid, "state": mp.state}

    async def stop(self, name: str, sig: str = "TERM") -> dict:
        if name not in self.named:
            raise KeyError(f"unknown tool {name!r}")
        mp = self.named[name]
        if mp.state == "stopped":
            return {"stopped": False, "last_exit_code": mp.last_exit_code}
        rc = await mp.stop(sig=sig, grace=self.config.shutdown_grace_seconds)
        return {"stopped": True, "last_exit_code": rc}

    def status(self, name: str | None = None) -> dict:
        if name is not None:
            mp = self.named.get(name)
            if not mp:
                raise KeyError(f"unknown tool {name!r}")
            return {"kind": "named_tool", **mp.status()}
        return {n: {"kind": "named_tool", **mp.status()} for n, mp in self.named.items()}

    def logs(
        self,
        target: str | int,
        n: int = 50,
        stream: str = "all",
        previous: bool = False,
    ) -> list[dict]:
        buf = self._resolve_log_target(target, previous=previous)
        if buf is None:
            return []
        return buf.tail(n, stream)  # type: ignore[return-value]

    def _resolve_log_target(
        self, target: str | int, previous: bool = False
    ) -> RingBuffer | None:
        if isinstance(target, int):
            if previous:
                raise ValueError("previous logs not supported for bash pids")
            if target in self.bash_pids:
                return self.bash_pids[target].log
            for mp in self.named.values():
                if mp.pid == target:
                    return mp.log
            raise KeyError(f"no buffer for pid {target}")
        if target in self.named:
            mp = self.named[target]
            return mp.previous_log if previous else mp.log
        raise KeyError(f"unknown log target {target!r}")

    # --- bash ---

    async def bash(
        self,
        cmd: str,
        timeout: float = 30.0,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
    ) -> dict:
        buf = RingBuffer(self.config.log_buffer)
        proc_env = {**os.environ, **(env or {})}
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=cwd,
            env=proc_env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        tracked = BashTracked(proc, buf)
        self._track_bash(proc.pid, tracked)

        t_out = asyncio.create_task(pump_stream(proc.stdout, buf, "stdout"))
        t_err = asyncio.create_task(pump_stream(proc.stderr, buf, "stderr"))
        start = time.monotonic()

        try:
            rc = await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return {
                "status": "timeout",
                "pid": proc.pid,
                "recent_logs": buf.tail(50),
            }
        await asyncio.gather(t_out, t_err, return_exceptions=True)
        duration = time.monotonic() - start
        tracked.exit_code = rc
        tracked.exited_at = time.monotonic()
        stdout = "\n".join(l["message"] for l in buf.tail(0, "stdout"))
        stderr = "\n".join(l["message"] for l in buf.tail(0, "stderr"))
        return {
            "exit_code": rc,
            "stdout": stdout,
            "stderr": stderr,
            "duration": duration,
        }

    def _track_bash(self, pid: int, tracked: BashTracked) -> None:
        self.bash_pids[pid] = tracked
        self.bash_pids.move_to_end(pid)
        while len(self.bash_pids) > BASH_PID_LRU:
            self.bash_pids.popitem(last=False)

    async def wait(self, pid: int, timeout: float = 60.0) -> dict:
        if pid not in self.bash_pids:
            raise KeyError(f"unknown pid {pid}")
        t = self.bash_pids[pid]
        start = time.monotonic()
        try:
            rc = await asyncio.wait_for(t.proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return {"status": "timeout"}
        t.exit_code = rc
        return {"exit_code": rc, "duration": time.monotonic() - start}

    async def kill(self, pid: int, sig: str = "TERM") -> dict:
        t = self.bash_pids.get(pid)
        if not t:
            raise KeyError(f"unknown pid {pid}")
        if t.proc.returncode is not None:
            return {"killed": False}
        try:
            t.proc.send_signal(getattr(signal, f"SIG{sig}", signal.SIGTERM))
        except ProcessLookupError:
            return {"killed": False}
        return {"killed": True}

    async def shutdown_all(self) -> None:
        for mp in self.named.values():
            if mp.state in ("running", "starting"):
                await mp.stop(grace=self.config.shutdown_grace_seconds)
        for t in list(self.bash_pids.values()):
            if t.proc.returncode is None:
                try:
                    t.proc.terminate()
                except ProcessLookupError:
                    pass
