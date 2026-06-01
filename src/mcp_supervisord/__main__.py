from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import uvicorn
from dotenv import load_dotenv

from .config import load_config
from .server import Supervisor


TOKEN_ENV = "DEVCONTAINER_TOKEN"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="supervisor-and-mcp-proxy")
    p.add_argument("--config", "-c", required=True, help="path to .supervisor.json")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    args = p.parse_args(argv)

    load_dotenv(Path(args.config).resolve().parent / ".env")
    load_dotenv()

    auth_token = os.environ.get(TOKEN_ENV, "").strip()
    if not auth_token:
        sys.stderr.write(
            f"error: {TOKEN_ENV} is not set. Create a .env file (next to your "
            f"config or in the working directory) with `{TOKEN_ENV}=<your-token>`.\n"
        )
        return 2

    cfg = load_config(args.config)
    host, port = cfg.host_port
    if args.host:
        host = args.host
    if args.port:
        port = args.port

    sup = Supervisor(cfg, config_path=args.config, auth_token=auth_token)
    app = sup.build_app()
    uvicorn.run(app, host=host, port=port, log_level="info")
    return 0


if __name__ == "__main__":
    sys.exit(main())
