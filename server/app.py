#!/usr/bin/env python3

from fastmcp import FastMCP
from fastmcp.contrib.mcp_mixin import MCPMixin

from server.config import Config
from server.health import register_healthchecks
from server.log_utils import async_log_request, async_log_response, setup_logging
from server.tools.auth_utils import create_base_client
from server.tools.bi_tools import BuyerIntentTools
from server.tools.category_tools import CategoryTools
from server.tools.product_tools import ProductTools
from server.tools.review_tools import ReviewTools

config = Config.from_env()
logger = setup_logging()


client = create_base_client(
    base_url=config.base_url,
    timeout=config.timeout,
    additional_headers={"Content-Type": "application/json"},
    debug_logging=config.debug,
    event_hooks={"request": [async_log_request], "response": [async_log_response]},
)

logger.info("Initializing G2 MCP Server")
logger.debug(f"Server config: base_url={config.base_url}, debug={config.debug}")

mcp: FastMCP = FastMCP(
    name="G2 MCP Server",
)
tools: list[MCPMixin] = [
    BuyerIntentTools(client),
    ReviewTools(client),
    ProductTools(client),
    CategoryTools(client),
]

for tool in tools:
    tool.register_all(mcp)

register_healthchecks(mcp)


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
