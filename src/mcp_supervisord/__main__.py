from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from .config import load_config
from .server import Supervisor


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="supervisor-and-mcp-proxy")
    p.add_argument("--config", "-c", required=True, help="path to .supervisor.json")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    args = p.parse_args(argv)

    load_dotenv(Path(args.config).resolve().parent / ".env")
    load_dotenv()

    cfg = load_config(args.config)
    if cfg.auth is not None and not cfg.auth.token:
        sys.stderr.write(
            "error: auth.token is empty. Set DEVCONTAINER_TOKEN in .env "
            "(next to your config or in the working directory) and reference it "
            'in the config as "token": "${DEVCONTAINER_TOKEN}".\n'
        )
        return 2
    host, port = cfg.host_port
    if args.host:
        host = args.host
    if args.port:
        port = args.port

    sup = Supervisor(cfg, config_path=args.config)
    app = sup.build_app()
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
