# supervisor-and-mcp-proxy — Design Spec

**Date:** 2026-05-31
**Status:** Draft / awaiting user review
**Scope:** New Python service `supervisor-and-mcp-proxy` providing a unified MCP HTTP endpoint that (a) proxies one or more stdio MCP servers and (b) manages local processes with structured log capture. Runs inside the project's devcontainer.

## 1. Goals

1. Expose multiple stdio MCP servers (e.g., Serena) over a single **Streamable HTTP** MCP endpoint reachable from Claude Desktop on the host.
2. Provide named-process lifecycle management (`start`, `stop`, `status`, `logs`) for declared tools like `mix phx.server`.
3. Provide ad-hoc `bash` execution that degrades gracefully on timeout (returns `pid` and recent logs; process keeps running) plus `wait` / `kill` follow-ups.
4. Maintain a per-process ring buffer of log lines with timestamps and stream attribution.
5. Implement automatic restart with crash-loop backoff for processes whose policy demands it (Serena = `on-failure`, Phoenix = `never`).

## 2. Non-Goals

- Persisting logs to disk. Buffer is RAM only; lines past `log_buffer` are dropped.
- Authenticating per-user / multi-tenant. Single shared bearer-style query token.
- Remote exposure beyond `localhost` (via devcontainer port-forward).
- Supervising the supervisor itself (use devcontainer `postStartCommand` + systemd-style autorestart later if needed).

## 3. Architecture

```
Claude Desktop (host)
        │ POST /mcp?token=…  (Streamable HTTP transport)
        ▼
┌──────────────────────────────────────┐
│  supervisor-and-mcp-proxy (uvicorn)           │
│  ┌────────────────────────────────┐  │
│  │ MCP Server                     │  │
│  │  - own tools                   │  │
│  │  - proxied tools (namespaced)  │  │
│  └────────────────────────────────┘  │
│  ┌──────────┐  ┌────────────────┐    │
│  │ Proxy    │  │ ProcessMgr     │    │
│  │ Manager  │  │ named_tools +  │    │
│  │ (stdio   │  │ bash-spawned   │    │
│  │ clients) │  │ RingBuf/pid    │    │
│  └──────────┘  └────────────────┘    │
└──────────────────────────────────────┘
       │ stdio                │ subprocess
       ▼                      ▼
   Serena, …            mix phx.server, bash …
```

Supervisor is a single `asyncio` process. Uvicorn hosts the MCP HTTP transport. Background tasks: per upstream MCP client, per managed-process supervisor loop.

## 4. Configuration — `.supervisor.json`

Located at repo root. Loaded once at startup. `${VAR}` placeholders expanded from environment at load time.

```json
{
  "bind": "0.0.0.0:9121",
  "auth": { "token": "...." },
  "log_buffer": 1000,
  "shutdown_grace_seconds": 10,

  "mcp_servers": {
    "serena": {
      "namespace": "serena",
      "command": "serena-mcp-server",
      "args": [],
      "env": {},
      "autostart": true,
      "restart_policy": {
        "mode": "on-failure",
        "max_restarts": 5,
        "window_seconds": 60,
        "backoff_seconds": 2
      }
    }
  },

  "named_tools": {
    "server": {
      "command": "mix phx.server",
      "cwd": "/workspaces/app",
      "env": { "MIX_ENV": "dev" },
      "autostart": false,
      "restart_policy": { "mode": "never" }
    }
  }
}
```

**Field semantics:**

- `bind` — uvicorn host:port.
- `auth.token` — required at runtime via `?token=…` query (or `Authorization: Bearer …` header). Omit the whole `auth` block to disable auth.
- `log_buffer` — ring-buffer capacity **per process** (lines).
- `shutdown_grace_seconds` — on SIGTERM: send SIGTERM to all children, wait this long, then SIGKILL.
- `mcp_servers.<name>` — a stdio MCP upstream:
  - `namespace` — tool/resource/prompt name prefix (`<ns>__<orig>`). Empty string = no prefix.
  - `command`, `args`, `env`, `cwd` — process spec.
  - `autostart` — start with supervisor.
  - `restart_policy` — see §5.
- `named_tools.<name>` — managed local process exposed via `start`/`stop`/`status`/`logs`.

`.supervisor.json` schema is documented in `supervisor-and-mcp-proxy/README.md` (deliverable).

## 5. Restart policy & crash-loop backoff

`restart_policy.mode`:
- `never` — manual lifecycle only.
- `on-failure` — restart iff `exit_code != 0`.
- `always` — restart on every exit.

**Backoff state machine** (applies when mode requires restart):

1. On exit-triggered restart: `restart_count += 1`, append timestamp to a deque windowed by `window_seconds`.
2. If `restart_count` within the window > `max_restarts` → state = `backoff`. Supervisor stops trying. Manual `start(tool)` clears the counter.
3. Otherwise: sleep `backoff_seconds`, respawn. State: `starting` → `running`.
4. If uptime since last spawn exceeds `window_seconds` without exit, the in-window counter is naturally drained.

## 6. MCP tools exposed by supervisor

| Tool | Args | Returns |
|---|---|---|
| `start` | `tool: str` | `{pid, state}`; error `"already running"` with current pid if applicable. |
| `stop` | `tool: str`, `signal: str = "TERM"` | `{stopped: bool, last_exit_code?: int}` |
| `status` | `tool: str \| null = null` | one entry or full map (see §7) |
| `logs` | `target: str \| int`, `n: int = 50`, `stream: "all"\|"stdout"\|"stderr" = "all"` | `[{time, stream, message, truncated?}, ...]` |
| `bash` | `cmd: str`, `timeout: int = 30`, `cwd: str \| null = null`, `env: object = {}` | success: `{exit_code, stdout, stderr, duration}`; timeout: `{status: "timeout", pid, recent_logs}` |
| `wait` | `pid: int`, `timeout: int = 60` | `{exit_code, duration}` or `{status: "timeout"}` |
| `kill` | `pid: int`, `signal: str = "TERM"` | `{killed: bool}` |

Plus **N proxied tools** auto-discovered from every running `mcp_servers.*` via `tools/list`, exposed under the configured namespace. Resources/prompts handled symmetrically (best-effort: skip if upstream errors).

If an upstream is not currently `running`, calls to its proxied tools return error `"upstream <name> not available, state=<state>"`.

## 7. `status` response shape

```json
{
  "serena": {
    "kind": "mcp_server",
    "state": "running",
    "pid": 1234,
    "started_at": "2026-05-31T12:00:00Z",
    "uptime_seconds": 530,
    "restart_count": 0,
    "last_exit_code": null,
    "last_exit_at": null,
    "tools_proxied": 23
  },
  "server": {
    "kind": "named_tool",
    "state": "stopped",
    "pid": null,
    "restart_count": 0,
    "last_exit_code": 1,
    "last_exit_at": "2026-05-31T11:55:12Z"
  }
}
```

`state` ∈ {`stopped`, `starting`, `running`, `crashed`, `backoff`}.

## 8. Log buffer

- Per-pid `collections.deque(maxlen=log_buffer)`.
- Line = `{time: ISO-8601 UTC, stream: "stdout"\|"stderr", message: str, truncated: bool}`.
- Reader pumps each stream by line; lines >8 KB are truncated and flagged.
- Lifetime: kept until process is reaped **and** at least one `logs(pid)` call has been served after exit, then evictable. (Simplification for MVP: keep until supervisor shutdown or 100-pid LRU cap — TBD during implementation, defaulting to LRU=100.)

## 9. Auth

ASGI middleware:
- If `auth.token` configured, every request to `/mcp*` must present `?token=…` or `Authorization: Bearer …`. Mismatch → 401.
- `/healthz` (liveness) — unauthenticated, returns `{status: "ok", uptime, servers: {...short...}}`.

## 10. Component layout

```
supervisor-and-mcp-proxy/
├── pyproject.toml          # uv-managed, deps: mcp, uvicorn, pydantic, anyio
├── README.md               # .supervisor.json schema reference + usage
├── src/mcp_supervisord/
│   ├── __init__.py
│   ├── __main__.py         # CLI entry
│   ├── config.py           # pydantic models + ${VAR} expansion
│   ├── logbuf.py           # RingBuffer, LineReader
│   ├── process.py          # ManagedProcess (single-process lifecycle)
│   ├── manager.py          # ProcessManager (registry + bash pids)
│   ├── proxy.py            # UpstreamMCP (stdio client + cache)
│   ├── server.py           # MCP server, tool dispatch
│   ├── auth.py             # ASGI token middleware
│   └── shutdown.py         # signal handlers, graceful drain
└── tests/                  # see §12
```

## 11. Devcontainer integration

`supervisor-and-mcp-proxy` is installed and started by the devcontainer:

- `postCreateCommand` adds: `uv tool install --editable ./supervisor-and-mcp-proxy` (so `supervisor-and-mcp-proxy` CLI is on PATH).
- `postStartCommand`: `nohup supervisor-and-mcp-proxy --config /workspaces/app/.supervisor.json > /tmp/supervisord.log 2>&1 &`
- `forwardPorts`: append `9121`.

Claude Desktop config (host) points to `http://localhost:9121/mcp?token=…`.

## 12. Testing strategy

TDD: write failing test, implement, repeat.

| Suite | What it verifies |
|---|---|
| `test_config` | `${VAR}` expansion; pydantic validation; defaults; rejection of malformed restart policies. |
| `test_logbuf` | ring overflow drops oldest; per-stream attribution; >8 KB lines truncated and flagged. |
| `test_process` | exit code propagation; SIGTERM/SIGKILL paths; `on-failure` restarts on non-zero, skips on zero; `never` never restarts; crash-loop backoff trips after `max_restarts` within window; manual `start` clears backoff. |
| `test_proxy` | stub stdio MCP server (in-repo) — handshake, `tools/list`, `tools/call`, reconnect after upstream restart, namespace prefix applied. |
| `test_bash` | sync success path; timeout returns pid + recent_logs; subsequent `wait`/`kill` work; `kill` of nonexistent pid errors cleanly. |
| `test_e2e` | uvicorn on ephemeral port, real MCP HTTP client SDK, full handshake + `status` + `bash` + one proxied call. |

CI target: `uv run pytest -q` from `supervisor-and-mcp-proxy/`.

## 13. Deliverables

1. `.supervisor.json` at repo root (✅ already written).
2. `supervisor-and-mcp-proxy/` package per §10.
3. `supervisor-and-mcp-proxy/README.md` documenting full `.supervisor.json` schema, supervisor tools (§6), `status` shape (§7), Claude Desktop wiring snippet.
4. `.devcontainer/devcontainer.json` updated per §11.
5. Test suites per §12, green.

## 14. Open / deferred items

- **Log retention after reap:** §8 marks LRU=100 as MVP default; revisit if memory pressure observed.
- **Resource/prompt proxying:** implement same-shape as tools; skip if first upstream balks. Acceptable for MVP.
- **Exponential backoff vs fixed:** MVP uses fixed `backoff_seconds`. Multiplier deferred.
- **Persistent restart counters:** lost across supervisor restarts. Acceptable for MVP.
