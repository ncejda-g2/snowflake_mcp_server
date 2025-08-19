#!/usr/bin/env python3

from server.app import mcp
from server.config import Config

if __name__ == "__main__":
    config = Config.from_env()
    mcp.run(transport=config.transport, host=config.host, port=config.port)
