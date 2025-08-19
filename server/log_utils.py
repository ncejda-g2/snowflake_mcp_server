#!/usr/bin/env python3
"""Logging utilities for Snowflake MCP Server."""

import logging
import sys
from typing import Optional

from server.config import Config


def setup_logging(config: Optional[Config] = None) -> logging.Logger:
    """
    Set up logging configuration and return a logger instance.
    
    Args:
        config: Optional configuration object
        
    Returns:
        Configured logger instance
    """
    if not config:
        config = Config.from_env()
    
    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG if config.debug else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stderr)
        ]
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