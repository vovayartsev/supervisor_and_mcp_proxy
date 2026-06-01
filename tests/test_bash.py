from __future__ import annotations

import pytest

from mcp_supervisord.config import SupervisorConfig
from mcp_supervisord.manager import ProcessManager


def _mgr() -> ProcessManager:
    return ProcessManager(SupervisorConfig(log_buffer=100))


@pytest.mark.asyncio
async def test_bash_success_path():
    m = _mgr()
    res = await m.bash("echo hi && echo err >&2", timeout=5)
    assert res["exit_code"] == 0
    assert "hi" in res["stdout"]
    assert "err" in res["stderr"]
    assert res["duration"] >= 0


@pytest.mark.asyncio
async def test_bash_timeout_returns_pid_and_logs():
    m = _mgr()
    res = await m.bash("echo before; sleep 5", timeout=0.3)
    assert res["status"] == "timeout"
    assert isinstance(res["pid"], int)
    # recent_logs may or may not include the echo depending on flush timing
    assert isinstance(res["recent_logs"], list)
    # follow-up kill must work
    killed = await m.kill(res["pid"])
    assert killed["killed"] is True


@pytest.mark.asyncio
async def test_wait_unknown_pid_errors():
    m = _mgr()
    with pytest.raises(KeyError):
        await m.wait(999999)


@pytest.mark.asyncio
async def test_kill_unknown_pid_errors():
    m = _mgr()
    with pytest.raises(KeyError):
        await m.kill(999999)


@pytest.mark.asyncio
async def test_wait_after_timeout_yields_exit_code():
    m = _mgr()
    res = await m.bash("sleep 0.5", timeout=0.1)
    assert res["status"] == "timeout"
    pid = res["pid"]
    w = await m.wait(pid, timeout=3)
    assert w["exit_code"] == 0
