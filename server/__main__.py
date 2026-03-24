#!/usr/bin/env python3
"""Entry point for Snowflake MCP Server.

Validates required environment variables before importing server.app,
which calls Config.from_env() at module level.

If startup fails for any reason (missing env vars, import errors, config
errors), the server starts in **degraded mode** — a minimal MCP server
with a single `server_status` tool that reports the error. This ensures
MCP clients always connect successfully and agents get a clear error
message instead of silent failure.
"""

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Callable
from typing import Any

REQUIRED_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USERNAME",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_ROLE",
]


def _log(message: str, logger: logging.Logger | None = None) -> None:
    """Log to logger if available, always write to stderr as fallback."""
    if logger:
        logger.error(message)
    print(message, file=sys.stderr)


def _start_degraded(error_message: str, log_file_path: str | None = None) -> None:
    """Start a minimal MCP server that reports the startup error.

    Instead of sys.exit(1), this keeps the server alive so MCP clients
    complete the handshake. Agents see the error when calling any tool.
    """
    try:
        from fastmcp import FastMCP
    except ImportError:
        print(f"FATAL: {error_message}", file=sys.stderr)
        print(
            "Additionally, fastmcp is not installed — cannot start degraded server.",
            file=sys.stderr,
        )
        sys.exit(1)

    degraded = FastMCP("Snowflake Read-Only MCP (STARTUP ERROR)")

    _error = error_message
    _log_file = log_file_path

    @degraded.tool(
        name="server_status",
        description=(
            "⚠️ Server failed to start. Call this tool to see what went wrong "
            "and how to fix it."
        ),
    )
    def server_status() -> str:
        """Report the startup error and remediation steps."""
        lines = [
            "❌ Snowflake MCP Server failed to start.",
            "",
            f"Error: {_error}",
            "",
            "How to fix:",
            "  1. Check your MCP client configuration (env vars, command, args)",
            "  2. Ensure all required environment variables are set:",
            "     - SNOWFLAKE_ACCOUNT  (e.g. xy12345.us-east-1)",
            "     - SNOWFLAKE_USERNAME (e.g. user@company.com)",
            "     - SNOWFLAKE_WAREHOUSE (e.g. COMPUTE_WH)",
            "     - SNOWFLAKE_ROLE (e.g. ANALYST)",
            "  3. After fixing, restart your MCP client to reload the server.",
        ]
        if _log_file:
            lines.insert(-1, f"  Log file: {_log_file}")
        return "\n".join(lines)

    # Register stubs for all real tool names so agents get the error
    # instead of "tool not found"
    _real_tool_names = [
        ("refresh_catalog", "Refresh the schema catalog"),
        ("show_tables", "Browse databases, schemas, and tables"),
        ("find_tables", "Search for tables by keyword"),
        ("describe_table", "Get column information for a table"),
        ("execute_query", "Execute a read-only SQL query"),
        ("validate_query_without_execution", "Validate a SQL query"),
        ("get_query_history", "Get history of executed queries"),
        ("save_last_query_to_csv", "Save query results to CSV"),
        ("execute_big_query_to_disk", "Execute large query to disk"),
    ]

    for tool_name, tool_desc in _real_tool_names:

        def _make_stub(name: str, desc: str) -> Callable[..., str]:
            def stub() -> str:
                return (
                    f"❌ Cannot run '{name}' — server failed to start.\n\n"
                    f"Error: {_error}\n\n"
                    "Call the 'server_status' tool for full details and fix instructions."
                )

            stub.__name__ = name
            stub.__doc__ = f"⚠️ UNAVAILABLE — {desc} (server failed to start)"
            return stub

        degraded.tool(name=tool_name, description=f"⚠️ UNAVAILABLE: {tool_desc}")(
            _make_stub(tool_name, tool_desc)
        )

    print(f"Starting in degraded mode: {_error}", file=sys.stderr)
    try:
        degraded.run(transport="stdio")
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Degraded server also failed: {e}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    """Start the MCP server after validating environment."""
    # Phase 1: Try to set up logging (imports pydantic via server.config)
    logger = None
    log_file_path = None
    try:
        from server.log_utils import LOG_FILE_PATH
        from server.log_utils import logger as _logger

        logger = _logger
        log_file_path = str(LOG_FILE_PATH)
    except Exception as e:
        print(f"Warning: Could not initialize logging: {e}", file=sys.stderr)

    # Phase 2: Validate required environment variables
    missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
    if missing_vars:
        error_msg = (
            f"Missing required environment variables: {', '.join(missing_vars)}. "
            "Set them in your MCP client's env configuration."
        )
        _log(error_msg, logger)
        _start_degraded(error_msg, log_file_path)
        return

    # Phase 3: Try to import the full server
    try:
        from server.app import config, mcp
    except Exception as e:
        error_msg = f"Failed to initialize server: {e}"
        _log(error_msg, logger)
        _start_degraded(error_msg, log_file_path)
        return

    # Phase 4: Normal startup
    if logger:
        logger.info(
            "snowflake-mcp starting (account=%s, warehouse=%s, log file: %s)",
            os.getenv("SNOWFLAKE_ACCOUNT"),
            os.getenv("SNOWFLAKE_WAREHOUSE"),
            log_file_path,
        )

    try:
        if logger:
            logger.info("Starting MCP server (transport=%s)", config.transport)
        if config.transport == "stdio":
            mcp.run(transport="stdio")
        elif config.transport == "http":
            mcp.run(transport="http", host=config.host, port=config.port)
        else:
            error_msg = f"Unknown transport: {config.transport}"
            _log(error_msg, logger)
            sys.exit(1)
    except KeyboardInterrupt:
        if logger:
            logger.info("Server stopped by user")
        sys.exit(0)
    except Exception as e:
        if logger:
            logger.exception("Server error: %s", e)
        else:
            print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
