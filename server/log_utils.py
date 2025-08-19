#!/usr/bin/env python3

import logging

import httpx

from server.config import Config

config = Config.from_env()
logger = logging.getLogger(__name__)


def setup_logging() -> logging.Logger:
    """Set up logging configuration and return a logger instance."""
    logging.basicConfig(
        level=logging.DEBUG if config.debug else logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    return logging.getLogger("g2_mcp_server")


def log_request(request: httpx.Request) -> None:
    """Log outgoing HTTP requests."""
    logger.info(f"Request: {request.method} {request.url}")
    logger.debug(f"Request headers: {dict(request.headers)}")
    if request.content:
        logger.debug(
            f"Request body: {request.content.decode('utf-8', errors='ignore')}"
        )


def log_response(response: httpx.Response) -> None:
    """Log incoming HTTP responses."""
    logger.info(f"Response: {response.status_code} {response.url}")
    logger.debug(f"Response headers: {dict(response.headers)}")
    # Don't try to read response body in event hooks as it can cause issues
    # The body will be consumed by the actual request handler


async def async_log_request(request: httpx.Request) -> None:
    """Log outgoing HTTP requests (async version)."""
    logger.info(f"Request: {request.method} {request.url}")
    logger.debug(f"Request headers: {dict(request.headers)}")
    if request.content:
        logger.debug(
            f"Request body: {request.content.decode('utf-8', errors='ignore')}"
        )


async def async_log_response(response: httpx.Response) -> None:
    """Log incoming HTTP responses (async version)."""
    logger.info(f"Response: {response.status_code} {response.url}")
    logger.debug(f"Response headers: {dict(response.headers)}")
    # Don't try to read response body in event hooks as it can cause issues
    # The body will be consumed by the actual request handler
