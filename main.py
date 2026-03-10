#!/usr/bin/env python3
"""Main entry point for Snowflake MCP Server."""

import os
import sys

from server.log_utils import LOG_FILE_PATH, logger

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USERNAME",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
]


def main():
    """Main entry point."""
    missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]

    if missing_vars:
        logger.error("Missing required environment variables:")
        for var in missing_vars:
            logger.error("  - %s", var)
        logger.error(
            "Please set these environment variables before starting the server."
        )
        logger.error("Example:")
        logger.error('  export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"')
        logger.error('  export SNOWFLAKE_USERNAME="user@company.com"')
        logger.error('  export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"')
        logger.error('  export SNOWFLAKE_ROLE="YOUR_ROLE"')
        sys.exit(1)

    # Import app only after env vars are validated — app.py calls Config.from_env() at module level
    from server.app import config, mcp

    logger.info(
        "snowflake-mcp starting (account=%s, warehouse=%s, log file: %s)",
        os.getenv("SNOWFLAKE_ACCOUNT"),
        os.getenv("SNOWFLAKE_WAREHOUSE"),
        LOG_FILE_PATH,
    )

    try:
        logger.info("Starting MCP server (transport=%s)", config.transport)
        if config.transport == "stdio":
            mcp.run(transport="stdio")
        elif config.transport == "http":
            mcp.run(transport="http", host=config.host, port=config.port)
        else:
            logger.error("Unknown transport: %s", config.transport)
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.exception("Server error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
