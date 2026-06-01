from __future__ import annotations

import asyncio
import fcntl
import os
import pty
import re
import shlex
import signal
import struct
import termios
import time
from collections import OrderedDict
from typing import Any

from .logbuf import MAX_LINE_BYTES, RingBuffer

DEFAULT_COLS = 120
DEFAULT_ROWS = 30
SESSION_LRU = 50


def _set_winsize(fd: int, cols: int, rows: int) -> None:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))


class InteractiveSession:
    def __init__(
        self,
        sid: str,
        cmd: str,
        cwd: str | None,
        env: dict[str, str] | None,
        cols: int,
        rows: int,
        log_capacity: int,
    ) -> None:
        self.sid = sid
        self.cmd = cmd
        self.cwd = cwd
        self.env_overrides = env or {}
        self.cols = cols
        self.rows = rows
        self.log = RingBuffer(log_capacity)
        self.master_fd: int = -1
        self.pid: int = -1
        self.started_at = time.monotonic()
        self.exit_code: int | None = None
        self.exited_at: float | None = None
        self._partial = ""
        self._partial_at: float | None = None
        self._reader_attached = False
        self._exit_event = asyncio.Event()
        self._waiters: list[asyncio.Future[None]] = []
        self._waitpid_task: asyncio.Task | None = None

    async def spawn(self) -> None:
        master_fd, slave_fd = pty.openpty()
        _set_winsize(master_fd, self.cols, self.rows)
        argv = shlex.split(self.cmd) if self.cmd else ["/bin/sh"]
        proc_env = {**os.environ, **self.env_overrides}
        proc_env.setdefault("TERM", "xterm-256color")
        pid = os.fork()
        if pid == 0:  # child
            try:
                os.close(master_fd)
                os.setsid()
                fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)
                os.dup2(slave_fd, 0)
                os.dup2(slave_fd, 1)
                os.dup2(slave_fd, 2)
                if slave_fd > 2:
                    os.close(slave_fd)
                if self.cwd:
                    os.chdir(self.cwd)
                os.execvpe(argv[0], argv, proc_env)
            except Exception as e:  # pragma: no cover
                os.write(2, f"exec failed: {e}\n".encode())
                os._exit(127)
        # parent
        os.close(slave_fd)
        flags = fcntl.fcntl(master_fd, fcntl.F_GETFL)
        fcntl.fcntl(master_fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        self.master_fd = master_fd
        self.pid = pid
        loop = asyncio.get_running_loop()
        loop.add_reader(master_fd, self._on_readable)
        self._reader_attached = True
        self._waitpid_task = asyncio.create_task(self._reap())

    async def _reap(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                pid, status = await loop.run_in_executor(
                    None, lambda: os.waitpid(self.pid, os.WNOHANG)
                )
            except ChildProcessError:
                break
            if pid == 0:
                await asyncio.sleep(0.1)
                continue
            if os.WIFEXITED(status):
                self.exit_code = os.WEXITSTATUS(status)
            elif os.WIFSIGNALED(status):
                self.exit_code = -os.WTERMSIG(status)
            else:
                self.exit_code = -1
            break
        self.exited_at = time.monotonic()
        self._flush_partial(force=True)
        self._detach()
        self._exit_event.set()
        for f in self._waiters:
            if not f.done():
                f.set_result(None)
        self._waiters.clear()

    def _on_readable(self) -> None:
        try:
            data = os.read(self.master_fd, 65536)
        except (OSError, BlockingIOError):
            return
        if not data:
            self._detach()
            return
        self._absorb(data)

    def _absorb(self, data: bytes) -> None:
        text = data.decode("utf-8", errors="replace")
        self._partial += text
        # split on \n; \r alone is treated as carriage return (kept)
        while "\n" in self._partial:
            line, self._partial = self._partial.split("\n", 1)
            line = line.rstrip("\r")
            truncated = False
            if len(line.encode("utf-8", errors="replace")) > MAX_LINE_BYTES:
                line = line[:MAX_LINE_BYTES]
                truncated = True
            self.log.append("stdout", line, truncated)
        if self._partial:
            self._partial_at = time.monotonic()
        else:
            self._partial_at = None
        # notify waiters
        for f in self._waiters:
            if not f.done():
                f.set_result(None)
        self._waiters.clear()

    def _flush_partial(self, force: bool = False) -> None:
        if not self._partial:
            return
        if force:
            self.log.append("stdout", self._partial.rstrip("\r"))
            self._partial = ""
            self._partial_at = None

    def _detach(self) -> None:
        if not self._reader_attached:
            return
        loop = asyncio.get_running_loop()
        try:
            loop.remove_reader(self.master_fd)
        except Exception:
            pass
        try:
            os.close(self.master_fd)
        except OSError:
            pass
        self._reader_attached = False

    def write(self, data: str, add_newline: bool = True) -> int:
        if self.exit_code is not None or not self._reader_attached:
            raise RuntimeError(f"session {self.sid} not alive")
        payload = data + ("\n" if add_newline and not data.endswith("\n") else "")
        return os.write(self.master_fd, payload.encode("utf-8"))

    def resize(self, cols: int, rows: int) -> None:
        if self.master_fd >= 0:
            _set_winsize(self.master_fd, cols, rows)
            self.cols, self.rows = cols, rows

    async def wait_for_output(self, timeout: float) -> bool:
        if self.exit_code is not None:
            return False
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        self._waiters.append(fut)
        try:
            await asyncio.wait_for(fut, timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def wait_for_pattern(
        self, pattern: re.Pattern[str], timeout: float
    ) -> str | None:
        """Wait until cumulative output (incl. partial) matches pattern. Returns match text or None."""
        deadline = time.monotonic() + timeout
        while True:
            blob = self._tail_text()
            m = pattern.search(blob)
            if m:
                return m.group(0)
            if self.exit_code is not None:
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await self.wait_for_output(min(remaining, 1.0))

    async def wait_exit(self, timeout: float) -> int | None:
        if self.exit_code is not None:
            return self.exit_code
        try:
            await asyncio.wait_for(self._exit_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        return self.exit_code

    def _tail_text(self, n: int = 200) -> str:
        lines = [l["message"] for l in self.log.tail(n)]
        if self._partial:
            lines.append(self._partial)
        return "\n".join(lines)

    def snapshot(self, n: int = 50) -> dict[str, Any]:
        return {
            "session_id": self.sid,
            "pid": self.pid,
            "cmd": self.cmd,
            "alive": self.exit_code is None,
            "exit_code": self.exit_code,
            "uptime_seconds": (self.exited_at or time.monotonic()) - self.started_at,
            "lines": self.log.tail(n),
            "partial": self._partial,
            "cols": self.cols,
            "rows": self.rows,
        }

    async def close(self, sig: str = "TERM", grace: float = 5.0) -> int | None:
        if self.exit_code is not None:
            return self.exit_code
        signum = getattr(signal, f"SIG{sig}", signal.SIGTERM)
        self._signal_tree(signum)
        rc = await self.wait_exit(grace)
        if rc is None:
            self._signal_tree(signal.SIGKILL)
            rc = await self.wait_exit(2.0)
        return rc

    def _signal_tree(self, signum: int) -> None:
        """Signal child's process group. We use kill(-pid, sig) which targets pgid==pid
        (true because child called setsid). Never killpg(getpgid(pid)) from parent — that
        races setsid and can return the PARENT's pgid, killing the supervisor itself."""
        try:
            os.kill(-self.pid, signum)
        except ProcessLookupError:
            pass
        except PermissionError:
            # setsid hasn't completed yet → fall back to direct pid kill
            try:
                os.kill(self.pid, signum)
            except ProcessLookupError:
                pass


class InteractiveRegistry:
    def __init__(self, log_capacity: int) -> None:
        self.log_capacity = log_capacity
        self._sessions: OrderedDict[str, InteractiveSession] = OrderedDict()
        self._counter = 0

    def _new_id(self) -> str:
        self._counter += 1
        return f"int-{self._counter}"

    async def start(
        self,
        cmd: str,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        cols: int = DEFAULT_COLS,
        rows: int = DEFAULT_ROWS,
    ) -> InteractiveSession:
        sid = self._new_id()
        sess = InteractiveSession(sid, cmd, cwd, env, cols, rows, self.log_capacity)
        await sess.spawn()
        self._sessions[sid] = sess
        self._sessions.move_to_end(sid)
        self._evict_dead()
        return sess

    def _evict_dead(self) -> None:
        while len(self._sessions) > SESSION_LRU:
            for sid, s in list(self._sessions.items()):
                if s.exit_code is not None:
                    self._sessions.pop(sid)
                    break
            else:
                break

    def get(self, sid: str) -> InteractiveSession:
        if sid not in self._sessions:
            raise KeyError(f"unknown interactive session {sid!r}")
        return self._sessions[sid]

    def list(self) -> list[dict[str, Any]]:
        return [
            {
                "session_id": s.sid,
                "pid": s.pid,
                "cmd": s.cmd,
                "alive": s.exit_code is None,
                "exit_code": s.exit_code,
            }
            for s in self._sessions.values()
        ]

    async def shutdown_all(self) -> None:
        for s in list(self._sessions.values()):
            if s.exit_code is None:
                await s.close(grace=2.0)
