"""Authentication utilities for MCP tools."""

import httpx
from fastmcp.server.dependencies import get_http_headers

from server.log_utils import setup_logging

logging = setup_logging()


def create_base_client(
    base_url: str | None = None,
    timeout: float | None = None,
    additional_headers: dict[str, str] | None = None,
    debug_logging: bool = False,
    **kwargs,
) -> httpx.AsyncClient:
    """
    Create an httpx.AsyncClient without auth headers (auth will be applied per request).

    Args:
        base_url: Base URL for the client
        timeout: Request timeout
        additional_headers: Additional headers to include
        debug_logging: Whether to log debug information about headers
        **kwargs: Additional arguments to pass to httpx.AsyncClient

    Returns:
        Configured httpx.AsyncClient without auth headers
    """
    headers = additional_headers or {}

    if debug_logging:
        logging.debug(f"Base client initialized with headers: {headers}")

    client_kwargs = {"timeout": timeout, "headers": headers, **kwargs}
    if base_url is not None:
        client_kwargs["base_url"] = base_url

    return httpx.AsyncClient(**client_kwargs)


def get_auth_headers(debug_logging: bool = False) -> dict[str, str]:
    """
    Get authorization headers from MCP context as a dictionary.
    This function must be called within the context of a tool call.

    Args:
        debug_logging: Whether to log debug information about headers

    Returns:
        Dictionary containing authorization headers, or empty dict if none found
    """
    mcp_headers = get_http_headers()
    if mcp_headers is not None:
        auth_header = mcp_headers.get("authorization") or mcp_headers.get(
            "Authorization"
        )
        if auth_header:
            auth_headers = {"Authorization": auth_header}
            if debug_logging:
                logging.debug(f"MCP headers received: {mcp_headers}")
                logging.debug(f"Auth headers extracted: {auth_headers}")
            return auth_headers

    if debug_logging:
        logging.debug("No MCP headers found or no authorization header present")
    return {}


def merge_headers(*header_dicts: dict[str, str] | None) -> dict[str, str]:
    """
    Merge multiple header dictionaries, with later ones taking precedence.

    Args:
        *header_dicts: Variable number of header dictionaries to merge

    Returns:
        Merged headers dictionary
    """
    merged = {}
    for headers in header_dicts:
        if headers:
            merged.update(headers)
    return merged
