from __future__ import annotations

import asyncio

import pytest

from mcp_supervisord.config import RestartPolicy
from mcp_supervisord.process import ManagedProcess


async def _wait_state(mp: ManagedProcess, *states: str, timeout: float = 5.0) -> str:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if mp.state in states:
            return mp.state
        await asyncio.sleep(0.02)
    raise AssertionError(f"timeout waiting {states}, got {mp.state}")


@pytest.mark.asyncio
async def test_never_does_not_restart_on_failure():
    mp = ManagedProcess(
        name="t",
        command="sh",
        args=["-c", "exit 1"],
        env={},
        cwd=None,
        restart_policy=RestartPolicy(mode="never"),
        log_capacity=100,
    )
    await mp.start()
    await _wait_state(mp, "crashed", "stopped")
    await asyncio.sleep(0.3)
    assert mp.restart_count == 0
    assert mp.last_exit_code == 1


@pytest.mark.asyncio
async def test_on_failure_restarts_then_trips_backoff():
    mp = ManagedProcess(
        name="t",
        command="sh",
        args=["-c", "exit 1"],
        env={},
        cwd=None,
        restart_policy=RestartPolicy(
            mode="on-failure", max_restarts=2, window_seconds=60, backoff_seconds=0.05
        ),
        log_capacity=100,
    )
    await mp.start()
    await _wait_state(mp, "backoff", timeout=5.0)
    assert mp.restart_count >= 3
    # manual start clears
    await mp.start()
    assert mp.restart_count <= 3  # cleared, may rise again as failures re-fire


@pytest.mark.asyncio
async def test_on_failure_does_not_restart_zero_exit():
    mp = ManagedProcess(
        name="t",
        command="sh",
        args=["-c", "exit 0"],
        env={},
        cwd=None,
        restart_policy=RestartPolicy(mode="on-failure"),
        log_capacity=100,
    )
    await mp.start()
    await asyncio.sleep(0.3)
    assert mp.restart_count == 0
    assert mp.last_exit_code == 0


@pytest.mark.asyncio
async def test_previous_log_holds_prior_run_after_restart():
    mp = ManagedProcess(
        name="t",
        command="sh",
        args=["-c", "echo run-one; exit 1"],
        env={},
        cwd=None,
        restart_policy=RestartPolicy(
            mode="on-failure", max_restarts=10, window_seconds=60, backoff_seconds=0.05
        ),
        log_capacity=100,
    )
    await mp.start()
    # wait for at least one restart so previous_log is populated
    deadline = asyncio.get_event_loop().time() + 5.0
    while asyncio.get_event_loop().time() < deadline and mp.previous_log is None:
        await asyncio.sleep(0.05)
    assert mp.previous_log is not None
    prev_msgs = [l["message"] for l in mp.previous_log.tail(0)]
    assert any("run-one" in m for m in prev_msgs)
    await mp.stop(grace=1.0)


@pytest.mark.asyncio
async def test_stop_terminates_running_process():
    mp = ManagedProcess(
        name="t",
        command="sh",
        args=["-c", "sleep 30"],
        env={},
        cwd=None,
        restart_policy=RestartPolicy(mode="never"),
        log_capacity=100,
    )
    await mp.start()
    await _wait_state(mp, "running")
    assert mp.pid
    rc = await mp.stop(grace=2.0)
    assert mp.state == "stopped"
    assert rc is not None
