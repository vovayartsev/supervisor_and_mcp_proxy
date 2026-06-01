# supervisor-and-mcp-proxy

Unified MCP **Streamable HTTP** endpoint that:

1. Proxies one or more stdio MCP servers (Serena, etc.) under a single URL.
2. Supervises local processes (`mix phx.server`, …) with `start` / `stop` / `status` / `logs`.
3. Runs ad‑hoc `bash` commands that degrade to `pid + recent_logs` on timeout, then `wait` / `kill`.

All processes get a per-pid ring buffer of log lines with timestamps & stream attribution. Crash‑loop backoff is built in.

---

## Install / run

```sh
uv tool install --from git+https://github.com/vovayartsev/supervisor_and_mcp_proxy supervisor-and-mcp-proxy
```

CLI flags:

| Flag | Meaning |
|---|---|
| `-c, --config PATH` | path to `.supervisor.json` (required) |
| `--host HOST` | override `bind` host |
| `--port PORT` | override `bind` port |

---

## `.supervisor.json` schema

```json
{
  "bind": "0.0.0.0:9121",
  "auth": { "token": "..." },
  "log_buffer": 1000,
  "shutdown_grace_seconds": 10,

  "mcp_servers": {
    "<name>": {
      "namespace": "<name>",
      "command": "serena-mcp-server",
      "args": [],
      "env": {},
      "cwd": null,
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
    "<name>": {
      "command": "mix phx.server",
      "args": [],
      "env": { "MIX_ENV": "dev" },
      "cwd": "/workspaces/app",
      "autostart": false,
      "restart_policy": { "mode": "never" }
    }
  }
}
```

- `${VAR}` placeholders expand from environment at load time. Missing → empty string.
- `auth` is optional; omit to disable token gate.
- `command` for `mcp_servers` / `named_tools` may be a bare argv0 (`true`) or a full shell-quoted command. When `args` is provided, `command` is taken literally; when `args` is empty, `command` is `shlex.split`.
- `restart_policy.mode` ∈ `never` | `on-failure` | `always`.

Crash‑loop semantics: if more than `max_restarts` exit-triggered restarts happen within a rolling `window_seconds`, the process moves to state `backoff` and the supervisor stops trying. Calling `start(<tool>)` resets the counter.

---

## Supervisor tools

All exposed at `POST /mcp` (Streamable HTTP, MCP 2025‑06 spec).

| Tool | Args | Returns |
|---|---|---|
| `start` | `tool: str` | `{pid, state}` or `{error: "already running", pid, state}` |
| `stop` | `tool: str, signal="TERM"` | `{stopped, last_exit_code}` |
| `status` | `tool?: str` | one entry or full map (see below) |
| `logs` | `target: str\|int, n=50, stream="all"\|"stdout"\|"stderr", previous=false` | `[{time, stream, message, truncated?}, ...]` — buffer is per-run; `previous=true` returns the prior run's buffer (like `kubectl logs -p`), named tools / mcp_servers only. |
| `bash` | `cmd: str, timeout=30, cwd?, env={}` | `{exit_code, stdout, stderr, duration}` or `{status:"timeout", pid, recent_logs}` |
| `wait` | `pid: int, timeout=60` | `{exit_code, duration}` or `{status:"timeout"}` |
| `kill` | `pid: int, signal="TERM"` | `{killed: bool}` |

Each `mcp_servers.<ns>` upstream exports its own tools under prefix `<ns>__<orig>` (empty namespace = no prefix). Calls to a proxied tool while its upstream is not `running` return `{"error": "upstream <name> not available, state=<state>"}`.

### `status` shape

```json
{
  "serena": {
    "kind": "mcp_server",
    "state": "running",
    "pid": null,
    "started_at": "2026-05-31T12:00:00Z",
    "uptime_seconds": 530.4,
    "restart_count": 0,
    "last_exit_code": null,
    "last_exit_at": null,
    "tools_proxied": 23
  },
  "server": {
    "kind": "named_tool",
    "state": "stopped",
    "pid": null,
    "started_at": null,
    "uptime_seconds": null,
    "restart_count": 0,
    "last_exit_code": 1,
    "last_exit_at": "2026-05-31T11:55:12Z"
  }
}
```

`state` ∈ {`stopped`, `starting`, `running`, `crashed`, `backoff`}.

(`mcp_server` entries currently expose `pid=null`; the underlying `stdio_client` does not surface the child pid. Logs of the upstream's stderr are still captured.)

### Endpoints

- `POST /mcp` — MCP Streamable HTTP.
- `GET /healthz` — unauthenticated. Returns `{status, uptime, servers:{...}}`.

### Auth

If `auth.token` is set, every request to `/mcp*` must present **one** of:

- `?token=<value>` query parameter, or
- `Authorization: Bearer <value>` header.

Mismatch → `401 {"error": "unauthorized"}`.

---

## Claude Desktop wiring

`~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "devcontainer": {
      "transport": {
        "type": "streamable-http",
        "url": "http://localhost:9121/mcp?token=YOUR_TOKEN"
      }
    }
  }
}
```

From a devcontainer, forward port `9121` to the host (`forwardPorts: [9121]` in `.devcontainer/devcontainer.json`) and Claude Desktop reaches it via localhost.

---

## Logs

Each process owns a `collections.deque(maxlen=log_buffer)` of `{time, stream, message, truncated?}` lines (UTC ISO‑8601). The buffer is **per run**: on each (re)spawn the current buffer is moved to `previous_log` and a fresh one starts. Use `logs(target, previous=true)` to read the prior run (analogous to `kubectl logs -p`). Only one prior run is retained. Lines >8 KB are truncated and flagged. Buffers are RAM only; nothing is persisted to disk. Bash pid buffers are LRU‑capped at 100 and have no `previous` notion.

---

## Restart policy state machine

```
starting --(spawn ok)--> running --(exit)--> {crashed|stopped}
   ^                                              |
   |        mode=never:           stop here       |
   |        mode=on-failure & rc==0: stop here    |
   |        else: count++, drain window,          |
   |              if count>max → backoff,         |
   +--------------- else: sleep(backoff) ---------+
```

Manual `start(<tool>)` always clears the counter.

---

## Devcontainer integration (when ready)

Add to `.devcontainer/devcontainer.json`:

```json
{
  "forwardPorts": [9121],
  "postCreateCommand": "uv tool install --editable ./supervisor-and-mcp-proxy",
  "postStartCommand": "nohup supervisor-and-mcp-proxy --config /workspaces/app/.supervisor.json > /tmp/supervisord.log 2>&1 &"
}
```

---

## Testing

```sh
uv run pytest -q
```

Covers config (env expansion, validation), log ring buffer (overflow / truncation / stream attribution), managed process (restart modes, backoff, stop), bash (success, timeout → pid + logs, wait, kill, unknown-pid errors), and proxy handshake against an in‑repo stub MCP server.

---

## Limitations (MVP)

- Upstream `mcp_server` pids are not exposed in `status`.
- Restart counters are lost across supervisor restarts.
- Backoff is fixed (no exponential multiplier).
- Log lines past `log_buffer` are silently dropped (no disk spill).
- Single shared token; no per‑user auth.
