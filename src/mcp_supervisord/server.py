from __future__ import annotations

import contextlib
import json
import time
from datetime import datetime, timezone
from typing import Any

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .auth import TokenAuthMiddleware
from .config import SupervisorConfig
from .manager import ProcessManager
from .proxy import UpstreamMCP


SUPERVISOR_TOOLS: list[types.Tool] = [
    types.Tool(
        name="start",
        description="Start a named tool (managed process).",
        inputSchema={
            "type": "object",
            "properties": {"tool": {"type": "string"}},
            "required": ["tool"],
        },
    ),
    types.Tool(
        name="stop",
        description="Stop a named tool. signal default TERM.",
        inputSchema={
            "type": "object",
            "properties": {
                "tool": {"type": "string"},
                "signal": {"type": "string", "default": "TERM"},
            },
            "required": ["tool"],
        },
    ),
    types.Tool(
        name="status",
        description="Status of one tool or all (mcp_servers + named_tools).",
        inputSchema={
            "type": "object",
            "properties": {"tool": {"type": ["string", "null"]}},
        },
    ),
    types.Tool(
        name="logs",
        description=(
            "Tail recent log lines for target (tool name or bash pid). "
            "Buffer holds only the current run; pass previous=true (like `kubectl logs -p`) "
            "to read the prior run's buffer (named tools / mcp_servers only)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "target": {"type": ["string", "integer"]},
                "n": {"type": "integer", "default": 50},
                "stream": {"type": "string", "enum": ["all", "stdout", "stderr"], "default": "all"},
                "previous": {"type": "boolean", "default": False},
            },
            "required": ["target"],
        },
    ),
    types.Tool(
        name="bash",
        description="Run a shell command. On timeout returns pid + recent_logs; process keeps running.",
        inputSchema={
            "type": "object",
            "properties": {
                "cmd": {"type": "string"},
                "timeout": {"type": "integer", "default": 30},
                "cwd": {"type": ["string", "null"]},
                "env": {"type": "object", "default": {}},
            },
            "required": ["cmd"],
        },
    ),
    types.Tool(
        name="wait",
        description="Wait for a previously spawned bash pid to exit.",
        inputSchema={
            "type": "object",
            "properties": {
                "pid": {"type": "integer"},
                "timeout": {"type": "integer", "default": 60},
            },
            "required": ["pid"],
        },
    ),
    types.Tool(
        name="kill",
        description="Send a signal to a tracked bash pid.",
        inputSchema={
            "type": "object",
            "properties": {
                "pid": {"type": "integer"},
                "signal": {"type": "string", "default": "TERM"},
            },
            "required": ["pid"],
        },
    ),
    types.Tool(
        name="restart_supervisor",
        description="Gracefully stop all managed processes, reload .supervisor.json from disk, restart with new config.",
        inputSchema={"type": "object", "properties": {}},
    ),
]

SUPERVISOR_TOOL_NAMES = {t.name for t in SUPERVISOR_TOOLS}


def _json_content(value: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(value, default=str))]


class Supervisor:
    def __init__(self, config: SupervisorConfig, config_path: str = "") -> None:
        self.config = config
        self.config_path = config_path
        self.manager = ProcessManager(config)
        self.upstreams: dict[str, UpstreamMCP] = {
            name: UpstreamMCP(name, spec, config.log_buffer)
            for name, spec in config.mcp_servers.items()
        }
        self.started_at = datetime.now(timezone.utc)
        self.server = self._build_server()
        self.session_manager = StreamableHTTPSessionManager(
            app=self.server,
            stateless=True,
            json_response=True,
        )

    # --- lifecycle ---

    async def autostart(self) -> None:
        for name, up in self.upstreams.items():
            if up.spec.autostart:
                await up.start()
        await self.manager.autostart()

    async def shutdown(self) -> None:
        for up in self.upstreams.values():
            await up.stop()
        await self.manager.shutdown_all()

    async def reload(self) -> dict:
        if not self.config_path:
            return {"error": "config_path not set; cannot reload"}
        await self.shutdown()
        from .config import load_config
        self.config = load_config(self.config_path)
        self.manager = ProcessManager(self.config)
        self.upstreams = {
            name: UpstreamMCP(name, spec, self.config.log_buffer)
            for name, spec in self.config.mcp_servers.items()
        }
        await self.autostart()
        return {"reloaded": True, "config": self.config_path}

    # --- MCP server wiring ---

    def _build_server(self) -> Server:
        server: Server = Server("supervisor-and-mcp-proxy")

        @server.list_tools()
        async def _list_tools() -> list[types.Tool]:
            tools = list(SUPERVISOR_TOOLS)
            for up in self.upstreams.values():
                for t in up.tools:
                    tools.append(
                        types.Tool(
                            name=up.prefix(t.name),
                            description=t.description,
                            inputSchema=t.inputSchema,
                        )
                    )
            return tools

        @server.call_tool()
        async def _call_tool(name: str, arguments: dict[str, Any]) -> list[types.TextContent]:
            if name in SUPERVISOR_TOOL_NAMES:
                result = await self._dispatch_own(name, arguments)
                return _json_content(result)
            for up in self.upstreams.values():
                orig = up.unprefix(name)
                if orig is None:
                    continue
                if any(t.name == orig for t in up.tools):
                    try:
                        res = await up.call_tool(orig, arguments)
                    except Exception as e:
                        return _json_content({"error": str(e)})
                    return [c for c in res.content if isinstance(c, types.TextContent)] or _json_content(
                        {"content_types": [type(c).__name__ for c in res.content]}
                    )
            raise ValueError(f"unknown tool {name!r}")

        return server

    async def _dispatch_own(self, name: str, args: dict[str, Any]) -> Any:
        try:
            if name == "start":
                return await self.manager.start(args["tool"])
            if name == "stop":
                return await self.manager.stop(args["tool"], args.get("signal", "TERM"))
            if name == "status":
                tool = args.get("tool")
                if tool is None:
                    out: dict[str, Any] = {}
                    for n, up in self.upstreams.items():
                        out[n] = up.status()
                    out.update(self.manager.status())
                    return out
                if tool in self.upstreams:
                    return self.upstreams[tool].status()
                return self.manager.status(tool)
            if name == "logs":
                target = args["target"]
                n = int(args.get("n", 50))
                stream = args.get("stream", "all")
                previous = bool(args.get("previous", False))
                if isinstance(target, str) and target in self.upstreams:
                    up = self.upstreams[target]
                    buf = up.previous_log if previous else up.log
                    return [] if buf is None else buf.tail(n, stream)
                return self.manager.logs(target, n, stream, previous=previous)
            if name == "bash":
                return await self.manager.bash(
                    args["cmd"],
                    timeout=float(args.get("timeout", 30)),
                    cwd=args.get("cwd"),
                    env=args.get("env") or {},
                )
            if name == "wait":
                return await self.manager.wait(int(args["pid"]), float(args.get("timeout", 60)))
            if name == "kill":
                return await self.manager.kill(int(args["pid"]), args.get("signal", "TERM"))
            if name == "restart_supervisor":
                return await self.reload()
        except KeyError as e:
            return {"error": str(e)}
        raise ValueError(f"unknown supervisor tool {name!r}")

    # --- ASGI app ---

    def build_app(self) -> Starlette:
        async def healthz(_request: Request) -> JSONResponse:
            return JSONResponse({
                "status": "ok",
                "uptime": (datetime.now(timezone.utc) - self.started_at).total_seconds(),
                "servers": {
                    n: {"state": up.state, "tools_proxied": up.tools_proxied}
                    for n, up in self.upstreams.items()
                },
            })

        async def mcp_handler(scope, receive, send):
            await self.session_manager.handle_request(scope, receive, send)

        @contextlib.asynccontextmanager
        async def lifespan(app):
            async with self.session_manager.run():
                await self.autostart()
                try:
                    yield
                finally:
                    await self.shutdown()

        routes = [
            Route("/healthz", healthz),
            Mount("/mcp", app=mcp_handler),
        ]
        app = Starlette(routes=routes, lifespan=lifespan)
        token = self.config.auth.token if self.config.auth else None
        return TokenAuthMiddleware(app, token)  # type: ignore[return-value]
