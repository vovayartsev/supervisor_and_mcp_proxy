from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from mcp_supervisord.config import (
    SupervisorConfig,
    expand_env,
    load_config,
)


def test_expand_env_string_and_nested():
    env = {"TOKEN": "secret", "EMPTY": ""}
    assert expand_env("a-${TOKEN}-b", env) == "a-secret-b"
    assert expand_env({"k": "${TOKEN}", "l": ["${EMPTY}", "x"]}, env) == {
        "k": "secret",
        "l": ["", "x"],
    }
    # unknown var -> ""
    assert expand_env("${MISSING}", env) == ""


def test_load_config_minimal(tmp_path: Path):
    cfg_file = tmp_path / ".supervisor.json"
    cfg_file.write_text(json.dumps({
        "bind": "127.0.0.1:1234",
        "mcp_servers": {
            "x": {"command": "true"}
        },
        "named_tools": {
            "y": {"command": "true"}
        }
    }))
    cfg = load_config(cfg_file, env={"TOK": "abc"})
    assert cfg.bind == "127.0.0.1:1234"
    assert cfg.host_port == ("127.0.0.1", 1234)
    assert cfg.mcp_servers["x"].autostart is True  # default for mcp_servers
    assert cfg.mcp_servers["x"].restart_policy.mode == "never"  # pydantic default
    assert cfg.named_tools["y"].autostart is False


def test_load_config_strips_comment_keys(tmp_path: Path):
    p = tmp_path / "c.json"
    p.write_text(json.dumps({"$comment": "hello", "bind": "0.0.0.0:9121"}))
    cfg = load_config(p)
    assert cfg.bind == "0.0.0.0:9121"


def test_restart_policy_rejects_bad_mode():
    with pytest.raises(ValidationError):
        SupervisorConfig.model_validate({
            "mcp_servers": {
                "x": {"command": "true", "restart_policy": {"mode": "bogus"}}
            }
        })


def test_restart_policy_rejects_negative_window():
    with pytest.raises(ValidationError):
        SupervisorConfig.model_validate({
            "mcp_servers": {
                "x": {"command": "true", "restart_policy": {"mode": "always", "window_seconds": -1}}
            }
        })
