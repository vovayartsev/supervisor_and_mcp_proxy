from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def expand_env(value: Any, env: dict[str, str] | None = None) -> Any:
    env = env if env is not None else os.environ
    if isinstance(value, str):
        return _VAR_RE.sub(lambda m: env.get(m.group(1), ""), value)
    if isinstance(value, list):
        return [expand_env(v, env) for v in value]
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    return value


class RestartPolicy(BaseModel):
    mode: Literal["never", "on-failure", "always"] = "never"
    max_restarts: int = 5
    window_seconds: int = 60
    backoff_seconds: float = 2.0

    @field_validator("max_restarts", "window_seconds")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("must be >= 0")
        return v


class AuthConfig(BaseModel):
    token: str


class McpServerSpec(BaseModel):
    namespace: str = ""
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    autostart: bool = True
    restart_policy: RestartPolicy = Field(default_factory=RestartPolicy)


class NamedToolSpec(BaseModel):
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
    autostart: bool = False
    restart_policy: RestartPolicy = Field(default_factory=RestartPolicy)


class SupervisorConfig(BaseModel):
    bind: str = "0.0.0.0:9121"
    auth: AuthConfig | None = None
    log_buffer: int = 1000
    shutdown_grace_seconds: float = 10.0
    mcp_servers: dict[str, McpServerSpec] = Field(default_factory=dict)
    named_tools: dict[str, NamedToolSpec] = Field(default_factory=dict)

    @property
    def host_port(self) -> tuple[str, int]:
        host, _, port = self.bind.rpartition(":")
        return host or "0.0.0.0", int(port)


def load_config(path: str | Path, env: dict[str, str] | None = None) -> SupervisorConfig:
    raw = json.loads(Path(path).read_text())
    raw = {k: v for k, v in raw.items() if not k.startswith("$")}
    raw = expand_env(raw, env)
    return SupervisorConfig.model_validate(raw)
