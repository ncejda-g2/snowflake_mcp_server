#!/usr/bin/env python3
"""Logging utilities for Snowflake MCP Server."""

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from server.config import Config

# Persistent log file location
LOG_DIR = Path.home() / ".snowflake_mcp"
LOG_FILE_PATH = LOG_DIR / "server.log"


def _create_file_handler() -> RotatingFileHandler | None:
    """Create a rotating file handler, or None if it fails."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        # Restrict directory to owner-only (may contain sensitive query logs)
        LOG_DIR.chmod(0o700)
        handler = RotatingFileHandler(
            LOG_FILE_PATH,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=2,
        )
        # Restrict log file to owner-only read/write
        LOG_FILE_PATH.chmod(0o600)
        return handler
    except Exception as e:
        # Never crash the server because of logging
        print(
            f"Warning: Could not create log file at {LOG_FILE_PATH}: {e}",
            file=sys.stderr,
        )
        return None


def setup_logging(config: Config | None = None) -> logging.Logger:
    """
    Set up logging configuration and return a logger instance.

    Logs are written to both stderr and ~/.snowflake-mcp/server.log.

    Args:
        config: Optional configuration object

    Returns:
        Configured logger instance
    """
    if not config:
        config = Config.from_env()

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_level = logging.DEBUG if config.debug else logging.INFO

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]

    file_handler = _create_file_handler()
    if file_handler:
        file_handler.setFormatter(logging.Formatter(log_format))
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers,
    )

    # Create logger for the application
    logger = logging.getLogger("snowflake_mcp_server")

    # Adjust snowflake connector logging (it can be verbose)
    if not config.debug:
        logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)

    return logger


# Create default logger instance
logger = setup_logging()
