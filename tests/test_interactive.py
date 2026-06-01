from __future__ import annotations

import re
import sys

import pytest

from mcp_supervisord.config import SupervisorConfig
from mcp_supervisord.manager import ProcessManager


def _mgr() -> ProcessManager:
    return ProcessManager(SupervisorConfig(log_buffer=200))


@pytest.mark.asyncio
async def test_interactive_echo_roundtrip():
    m = _mgr()
    sess = await m.interactive.start("cat")
    try:
        sess.write("hello pty")
        ok = await sess.wait_for_output(timeout=2)
        assert ok
        snap = sess.snapshot()
        joined = "\n".join(l["message"] for l in snap["lines"]) + snap["partial"]
        assert "hello pty" in joined
    finally:
        await sess.close(grace=1)


@pytest.mark.asyncio
async def test_interactive_partial_prompt_visible_without_newline():
    m = _mgr()
    # python -c 'print("Password: ", end=""); ...' — no trailing newline
    cmd = (
        f'{sys.executable} -c '
        '"import sys; sys.stdout.write(\'Password: \'); sys.stdout.flush(); '
        'import time; time.sleep(2)"'
    )
    sess = await m.interactive.start(cmd)
    try:
        matched = await sess.wait_for_pattern(re.compile(r"Password:\s*"), timeout=3)
        assert matched is not None
        snap = sess.snapshot()
        assert "Password:" in (snap["partial"] or "")
    finally:
        await sess.close(grace=1)


@pytest.mark.asyncio
async def test_interactive_wait_for_pattern_then_send(tmp_path):
    m = _mgr()
    script = tmp_path / "prompt.py"
    script.write_text(
        "import sys\n"
        "sys.stdout.write('name? ')\n"
        "sys.stdout.flush()\n"
        "n = sys.stdin.readline().strip()\n"
        "print('hi ' + n)\n"
    )
    sess = await m.interactive.start(f"{sys.executable} {script}")
    try:
        matched = await sess.wait_for_pattern(re.compile(r"name\?\s*"), timeout=3)
        assert matched is not None
        sess.write("alice")
        matched2 = await sess.wait_for_pattern(re.compile(r"hi alice"), timeout=3)
        assert matched2 is not None
        rc = await sess.wait_exit(timeout=3)
        assert rc == 0
    finally:
        await sess.close(grace=1)


@pytest.mark.asyncio
async def test_interactive_close_kills_process():
    m = _mgr()
    sess = await m.interactive.start(f'{sys.executable} -c "import time; time.sleep(30)"')
    rc = await sess.close(grace=2)
    assert rc is not None  # exited (likely -15 for SIGTERM)
    assert sess.exit_code is not None


@pytest.mark.asyncio
async def test_interactive_list_and_unknown_session():
    m = _mgr()
    sess = await m.interactive.start("cat")
    try:
        lst = m.interactive.list()
        assert any(s["session_id"] == sess.sid for s in lst)
        with pytest.raises(KeyError):
            m.interactive.get("nope")
    finally:
        await sess.close(grace=1)
