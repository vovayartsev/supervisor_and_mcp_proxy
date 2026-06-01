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


SERVER_INSTRUCTIONS = (
    "Supervisor + MCP proxy. Three kinds of subprocesses:\n"
    "  1. `named_tools` — long-lived processes declared in .supervisor.json. "
    "Lifecycle: stopped → starting → running → (stopped|crashed|backoff). "
    "On respawn the prior log buffer is preserved as `previous_log` "
    "(read with `logs target=<name> previous=true`, kubectl `-p` analogy). "
    "Manage via `start`/`stop`/`status`/`logs`.\n"
    "  2. `mcp_servers` — stdio MCP upstreams declared in .supervisor.json. "
    "Same lifecycle. Their tools are exposed here as `<namespace>__<orig>` "
    "(empty namespace = bare original name; collisions across namespaces are caller's problem). "
    "Calling a proxied tool whose upstream is not `running` returns "
    '`{\"error\": \"upstream <name> not available, state=<state>\"}`. '
    "Inspect with `status`/`logs`; restart by editing config and calling `restart_supervisor`.\n"
    "  3. Ad-hoc `bash` commands — fire-and-wait shell with timeout fallback. "
    "On timeout the process KEEPS RUNNING under a tracked pid; resume with `wait` or terminate with `kill`. "
    "Tracked pids LRU-evicted after 100 entries.\n"
    "Plus PTY-backed `start_interactive` sessions for REPLs/prompts (ssh, psql, etc.) "
    "driven via `interactive_send`/`interactive_read`/`interactive_close`/`interactive_list`.\n"
    "Log line shape: `{time, stream: 'stdout'|'stderr', message, truncated?}`. "
    "Times are ISO-8601 UTC. Durations are seconds (float). All timeouts are seconds."
)


SUPERVISOR_TOOLS: list[types.Tool] = [
    types.Tool(
        name="start",
        description=(
            "Start a named tool (long-lived managed process declared in .supervisor.json under `named_tools`). "
            "Idempotent: if already running, returns `{pid, state:'running', error:'already running'}` without respawning. "
            "Returns on success: `{pid:int, state:'running'|'starting'|'crashed'}`. "
            "Blocks until the first spawn attempt completes (success or crash). "
            "Side effect: starts a supervisor task that respawns per the tool's restart_policy "
            "(states: stopped→starting→running, then crashed/backoff on failure). "
            "Tail output with `logs target=<tool>`. Stop with `stop`."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tool": {
                    "type": "string",
                    "description": "Name of the named_tool from .supervisor.json (NOT an mcp_server or bash pid). Unknown name → `{error: \"unknown tool '<name>'\"}`.",
                },
            },
            "required": ["tool"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="stop",
        description=(
            "Stop a named tool. Sends `signal` (default SIGTERM), waits `shutdown_grace_seconds` from config, "
            "then SIGKILLs if still alive. Also cancels the supervisor/restart task so the tool stays down "
            "until `start` is called again. Returns `{stopped:true, last_exit_code:int|null}` on success, "
            "or `{stopped:false, last_exit_code}` if the tool was already stopped. "
            "Does NOT apply to bash pids — use `kill` for those — or to mcp_servers (restart_supervisor only)."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tool": {
                    "type": "string",
                    "description": "Name of the named_tool to stop.",
                },
                "signal": {
                    "type": "string",
                    "default": "TERM",
                    "description": "Signal name without the SIG prefix (e.g. 'TERM', 'INT', 'HUP', 'KILL'). Unknown name falls back to SIGTERM.",
                },
            },
            "required": ["tool"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="status",
        description=(
            "Inspect supervised processes. With no `tool`: returns a dict keyed by name covering BOTH "
            "`mcp_servers` (proxied upstreams) and `named_tools`. With `tool` set: returns just that entry. "
            "Each entry: `{kind:'mcp_server'|'named_tool', state, pid, started_at, uptime_seconds, "
            "restart_count, last_exit_code, last_exit_at}` plus `tools_proxied:int` for mcp_servers. "
            "States: 'stopped' | 'starting' | 'running' | 'crashed' | 'backoff' (backoff = exceeded "
            "restart_policy.max_restarts in window_seconds; will not auto-recover). "
            "Unknown name → `{error: \"unknown tool '<name>'\"}`. "
            "Does NOT list bash pids — those live only as logs targets."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "tool": {
                    "type": ["string", "null"],
                    "description": "Optional: name of a single named_tool or mcp_server. Omit/null to get all.",
                },
            },
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="logs",
        description=(
            "Tail recent log lines from a process's ring buffer. "
            "Returns `list[{time, stream, message, truncated?}]` (oldest→newest). "
            "Empty list if the target has no buffer yet (e.g. never-started named tool). "
            "Buffer capacity comes from config.log_buffer (per-process ring). "
            "`previous=true` reads the buffer captured at the LAST respawn boundary "
            "(`kubectl logs -p` analogy) — only valid for named_tools and mcp_servers, "
            "NOT for bash pids (raises). On a named_tool / mcp_server's first run, previous buffer is empty."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "target": {
                    "type": ["string", "integer"],
                    "description": (
                        "String: name of a named_tool or mcp_server. "
                        "Integer: a bash pid returned by `bash` (also matches a named_tool's current pid). "
                        "Unknown target → error."
                    ),
                },
                "n": {
                    "type": "integer",
                    "default": 50,
                    "description": "Max lines to return (tail). Use 0 to return all buffered lines.",
                },
                "stream": {
                    "type": "string",
                    "enum": ["all", "stdout", "stderr"],
                    "default": "all",
                    "description": "Filter by stream. 'all' interleaves both in original capture order.",
                },
                "previous": {
                    "type": "boolean",
                    "default": False,
                    "description": "Read the prior run's buffer (named_tool / mcp_server only). Errors for bash pids.",
                },
            },
            "required": ["target"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="bash",
        description=(
            "Run an ad-hoc shell command (via /bin/sh -c). "
            "On normal exit returns `{exit_code:int, stdout:str, stderr:str, duration:float}` (duration in seconds). "
            "On `timeout` returns `{status:'timeout', pid:int, recent_logs:list[LogLine]}` "
            "and the process KEEPS RUNNING — use `wait` to await exit or `kill` to terminate. "
            "Tracked pid is retained in an LRU (capacity 100) so `wait`/`kill`/`logs target=<pid>` work afterwards. "
            "stdin is /dev/null. Stdout/stderr are also captured to a per-pid ring buffer accessible via `logs`."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "cmd": {
                    "type": "string",
                    "description": "Shell command string. Passed to /bin/sh -c, so shell features (pipes, redirects, &&) work.",
                },
                "timeout": {
                    "type": "integer",
                    "default": 30,
                    "description": "Seconds to wait for the process to finish before returning a timeout sentinel. Process is NOT killed on timeout.",
                },
                "cwd": {
                    "type": ["string", "null"],
                    "description": "Working directory. Null/omitted = supervisor's cwd.",
                },
                "env": {
                    "type": "object",
                    "default": {},
                    "additionalProperties": {"type": "string"},
                    "description": "Extra env vars merged on top of the supervisor's process environment.",
                },
            },
            "required": ["cmd"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="wait",
        description=(
            "Wait for a tracked bash pid (from a prior `bash` timeout) to exit. "
            "Returns `{exit_code:int, duration:float}` on exit, or `{status:'timeout'}` if still running "
            "after `timeout` seconds (the process keeps running; call `wait` again or `kill`). "
            "Unknown/evicted pid → error. Does NOT apply to named_tool or interactive session pids."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pid": {
                    "type": "integer",
                    "description": "Bash pid returned by `bash` (must still be in the tracked LRU).",
                },
                "timeout": {
                    "type": "integer",
                    "default": 60,
                    "description": "Max seconds to block waiting for exit.",
                },
            },
            "required": ["pid"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="kill",
        description=(
            "Send a signal to a tracked bash pid. Returns `{killed:true}` if the signal was delivered, "
            "`{killed:false}` if the process had already exited or vanished. "
            "Non-blocking — to confirm the process is gone, follow with `wait`. "
            "For named_tool processes use `stop` instead; for PTY sessions use `interactive_close`."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "pid": {
                    "type": "integer",
                    "description": "Bash pid returned by `bash` (must still be in the tracked LRU).",
                },
                "signal": {
                    "type": "string",
                    "default": "TERM",
                    "description": "Signal name without SIG prefix (e.g. 'TERM','INT','KILL','HUP'). Unknown name falls back to SIGTERM.",
                },
            },
            "required": ["pid"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="start_interactive",
        description=(
            "Spawn a process attached to a fresh PTY so the caller can drive it interactively "
            "(prompts, REPLs, ssh, psql, fish, etc.). Returns `{session_id:str, pid:int}`. "
            "Drive the session with `interactive_send` / `interactive_read`; terminate with `interactive_close`. "
            "Sessions are tracked in an LRU (capacity 50); dead sessions are evicted lazily. "
            "Output is captured to a per-session ring buffer; unterminated trailing output is preserved "
            "as `partial` so prompts like `Password:` (no newline) are visible via `interactive_read`."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "cmd": {
                    "type": "string",
                    "description": "Command line, parsed with shlex (NOT a shell — use 'sh -c \"...\"' if you need pipes).",
                },
                "cwd": {
                    "type": ["string", "null"],
                    "description": "Working directory. Null/omitted = supervisor's cwd.",
                },
                "env": {
                    "type": "object",
                    "default": {},
                    "additionalProperties": {"type": "string"},
                    "description": "Extra env vars merged onto the supervisor's environment. TERM defaults to 'xterm-256color'.",
                },
                "cols": {
                    "type": "integer",
                    "default": 120,
                    "description": "PTY width in columns.",
                },
                "rows": {
                    "type": "integer",
                    "default": 30,
                    "description": "PTY height in rows.",
                },
            },
            "required": ["cmd"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="interactive_send",
        description=(
            "Write input to a PTY session and optionally wait for output. "
            "Returns a session snapshot: "
            "`{session_id, pid, cmd, alive:bool, exit_code:int|null, uptime_seconds, "
            "lines:list[LogLine], partial:str, cols, rows, matched:str|null}`. "
            "`partial` is the current unterminated line (e.g. a prompt awaiting input). "
            "`matched` is set when `wait_for` matched; null otherwise. "
            "If `wait_for` is provided, blocks up to `wait_timeout` seconds until the regex matches buffered output OR the process exits."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session id from `start_interactive` (e.g. 'int-3'). Unknown id → error.",
                },
                "input": {
                    "type": "string",
                    "description": "Bytes to write to the PTY master. Newline is NOT appended unless `add_newline` is true.",
                },
                "add_newline": {
                    "type": "boolean",
                    "default": True,
                    "description": "Append '\\n' to `input` before writing — set false to send raw bytes (control chars, partial lines).",
                },
                "wait_for": {
                    "type": ["string", "null"],
                    "description": "Optional Python regex (re.MULTILINE) to wait for in output. Null = don't wait for a pattern.",
                },
                "wait_timeout": {
                    "type": "number",
                    "default": 5,
                    "description": "Seconds to block waiting for `wait_for` or for any new output (falls back to 0.05s min if 0).",
                },
                "n": {
                    "type": "integer",
                    "default": 50,
                    "description": "Tail size for the returned `lines` array.",
                },
            },
            "required": ["session_id", "input"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="interactive_read",
        description=(
            "Read tail output from a PTY session WITHOUT writing. "
            "Returns the same snapshot shape as `interactive_send` (incl. `partial` for unterminated trailing output — "
            "essential for prompts like 'Password:' that emit no newline). "
            "If `wait_for` and `wait_timeout > 0`, blocks until the regex matches or timeout expires; "
            "if only `wait_timeout > 0`, blocks until any new output arrives or timeout expires; "
            "otherwise returns the current buffer immediately."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session id from `start_interactive`.",
                },
                "n": {
                    "type": "integer",
                    "default": 50,
                    "description": "Tail size for the returned `lines` array.",
                },
                "wait_for": {
                    "type": ["string", "null"],
                    "description": "Optional Python regex (re.MULTILINE) to wait for. Requires `wait_timeout > 0` to take effect.",
                },
                "wait_timeout": {
                    "type": "number",
                    "default": 0,
                    "description": "Seconds to block. 0 = return immediately with current buffer.",
                },
            },
            "required": ["session_id"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="interactive_close",
        description=(
            "Terminate a PTY session. Sends `signal` to the child's process group, waits up to `grace` seconds, "
            "then SIGKILLs if still alive. Returns `{closed:true, exit_code:int|null}`. "
            "Idempotent: closing an already-exited session returns its captured exit code."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "Session id from `start_interactive`.",
                },
                "signal": {
                    "type": "string",
                    "default": "TERM",
                    "description": "Signal name without SIG prefix. Sent to the session's process group (setsid'd child).",
                },
                "grace": {
                    "type": "number",
                    "default": 5,
                    "description": "Seconds to wait for graceful exit before escalating to SIGKILL.",
                },
            },
            "required": ["session_id"],
            "additionalProperties": False,
        },
    ),
    types.Tool(
        name="interactive_list",
        description=(
            "List all PTY sessions tracked in the registry (active and recently exited; LRU cap 50). "
            "Returns `list[{session_id, pid, cmd, alive, exit_code, uptime_seconds, cols, rows}]`. "
            "Use `interactive_read` to fetch buffered output for a specific session."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
    types.Tool(
        name="restart_supervisor",
        description=(
            "Gracefully shut down ALL supervised processes (named_tools, mcp_server upstreams, bash, interactive sessions), "
            "reload .supervisor.json from disk, and re-autostart everything declared with `autostart:true`. "
            "Returns `{reloaded:true, config:<path>}` on success or `{error:'config_path not set; cannot reload'}` "
            "if the server was started without a config path. "
            "Use this after editing .supervisor.json. Note: all bash pids and interactive session ids become invalid."
        ),
        inputSchema={"type": "object", "properties": {}, "additionalProperties": False},
    ),
]

SUPERVISOR_TOOL_NAMES = {t.name for t in SUPERVISOR_TOOLS}


def _json_content(value: Any) -> list[types.TextContent]:
    return [types.TextContent(type="text", text=json.dumps(value, default=str))]


class Supervisor:
    def __init__(
        self,
        config: SupervisorConfig,
        config_path: str = "",
        auth_token: str | None = None,
    ) -> None:
        self.config = config
        self.config_path = config_path
        self.auth_token = auth_token
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
        server: Server = Server(
            "supervisor-and-mcp-proxy",
            instructions=SERVER_INSTRUCTIONS,
        )

        @server.list_tools()
        async def _list_tools() -> list[types.Tool]:
            # Wait briefly for upstreams still warming up so the client doesn't
            # cache an incomplete tool list. Streamable HTTP stateless mode
            # cannot push notifications/tools/list_changed afterwards.
            import asyncio as _asyncio
            pending = [
                up for up in self.upstreams.values()
                if up.spec.autostart and (up.state in ("starting",) or not up.tools)
                and up.state != "backoff"
            ]
            if pending:
                await _asyncio.gather(
                    *(up.wait_ready(15.0) for up in pending),
                    return_exceptions=True,
                )
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
            if name == "start_interactive":
                sess = await self.manager.interactive.start(
                    cmd=args["cmd"],
                    cwd=args.get("cwd"),
                    env=args.get("env") or {},
                    cols=int(args.get("cols", 120)),
                    rows=int(args.get("rows", 30)),
                )
                return {"session_id": sess.sid, "pid": sess.pid}
            if name == "interactive_send":
                import re as _re
                sess = self.manager.interactive.get(args["session_id"])
                sess.write(args["input"], add_newline=bool(args.get("add_newline", True)))
                wf = args.get("wait_for")
                matched = None
                if wf:
                    pat = _re.compile(wf, _re.MULTILINE)
                    matched = await sess.wait_for_pattern(
                        pat, float(args.get("wait_timeout", 5))
                    )
                else:
                    await sess.wait_for_output(float(args.get("wait_timeout", 5)) or 0.05)
                snap = sess.snapshot(int(args.get("n", 50)))
                snap["matched"] = matched
                return snap
            if name == "interactive_read":
                import re as _re
                sess = self.manager.interactive.get(args["session_id"])
                wf = args.get("wait_for")
                matched = None
                wait_t = float(args.get("wait_timeout", 0))
                if wf and wait_t > 0:
                    pat = _re.compile(wf, _re.MULTILINE)
                    matched = await sess.wait_for_pattern(pat, wait_t)
                elif wait_t > 0:
                    await sess.wait_for_output(wait_t)
                snap = sess.snapshot(int(args.get("n", 50)))
                snap["matched"] = matched
                return snap
            if name == "interactive_close":
                sess = self.manager.interactive.get(args["session_id"])
                rc = await sess.close(
                    sig=args.get("signal", "TERM"),
                    grace=float(args.get("grace", 5)),
                )
                return {"closed": True, "exit_code": rc}
            if name == "interactive_list":
                return self.manager.interactive.list()
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
        return TokenAuthMiddleware(app, self.auth_token)  # type: ignore[return-value]
