from __future__ import annotations

import argparse
import asyncio
import sys

import uvicorn

from .config import load_config
from .server import Supervisor


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="supervisor-and-mcp-proxy")
    p.add_argument("--config", "-c", required=True, help="path to .supervisor.json")
    p.add_argument("--host", default=None)
    p.add_argument("--port", type=int, default=None)
    args = p.parse_args(argv)

    cfg = load_config(args.config)
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
