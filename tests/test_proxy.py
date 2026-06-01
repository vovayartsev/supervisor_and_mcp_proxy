from __future__ import annotations

import asyncio
import sys
import textwrap
from pathlib import Path

import pytest

from mcp_supervisord.config import McpServerSpec, RestartPolicy
from mcp_supervisord.proxy import UpstreamMCP


STUB_SERVER = textwrap.dedent(
    """
    import anyio
    from mcp.server.lowlevel import Server
    from mcp.server.stdio import stdio_server
    from mcp import types

    server = Server("stub")

    @server.list_tools()
    async def _lt():
        return [types.Tool(name="ping", description="pong", inputSchema={"type": "object"})]

    @server.call_tool()
    async def _ct(name, arguments):
        return [types.TextContent(type="text", text=f"pong:{name}")]

    async def main():
        async with stdio_server() as (r, w):
            await server.run(r, w, server.create_initialization_options())

    anyio.run(main)
    """
)


@pytest.mark.asyncio
async def test_proxy_handshake_lists_and_calls(tmp_path: Path):
    stub = tmp_path / "stub_server.py"
    stub.write_text(STUB_SERVER)

    spec = McpServerSpec(
        namespace="stub",
        command=sys.executable,
        args=[str(stub)],
        autostart=True,
        restart_policy=RestartPolicy(mode="never"),
    )
    up = UpstreamMCP("stub", spec, log_capacity=200)
    await up.start()
    try:
        # wait until tools loaded
        for _ in range(100):
            if up.state == "running" and up.tools:
                break
            await asyncio.sleep(0.05)
        assert up.state == "running", f"state={up.state}"
        assert any(t.name == "ping" for t in up.tools)
        assert up.prefix("ping") == "stub__ping"
        assert up.unprefix("stub__ping") == "ping"
        assert up.unprefix("other__ping") is None

        res = await up.call_tool("ping", {})
        texts = [c.text for c in res.content if hasattr(c, "text")]
        assert any("pong:ping" in t for t in texts)
    finally:
        await up.stop()
