"""Authentication refresh tool for Snowflake MCP Server."""

import logging
from typing import Any

from server.config import Config
from server.snowflake_connection import SnowflakeConnection

logger = logging.getLogger(__name__)


def refresh_snowflake_auth(
    connection: SnowflakeConnection | None,
    config: Config,
) -> dict[str, Any]:
    """
    Refresh Snowflake authentication using browser-based SSO.

    This tool attempts to re-authenticate with Snowflake using the configured
    authentication method. For browser-based SSO (externalbrowser or Okta),
    this will open a browser window for authentication.

    Use this tool when:
    - Authentication has expired
    - You need to refresh your session token
    - Connection errors indicate auth issues

    Returns:
        dict: Status message and any relevant information
    """
    try:
        logger.info("Attempting to refresh Snowflake authentication...")

        # Create a new connection instance
        new_connection = SnowflakeConnection(config)

        # Attempt to connect - this will use the configured authenticator
        new_connection.connect()

        # Test the connection
        test_result = new_connection.execute_query("SELECT CURRENT_USER() as user")

        if test_result.data:
            current_user = test_result.data[0].get("USER", "Unknown")
            logger.info(f"Authentication successful for user: {current_user}")

            # Close the new connection (we'll recreate it when needed)
            new_connection.disconnect()

            return {
                "status": "success",
                "message": f"Successfully authenticated as {current_user}",
                "user": current_user,
                "authenticator": config.authenticator,
                "instructions": (
                    "Authentication refreshed successfully. "
                    "The session token has been stored and will be reused for subsequent operations."
                )
            }
        else:
            raise Exception("Connection test failed - no data returned")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to refresh authentication: {error_msg}")

        # Provide helpful error messages based on the error type
        if "Password is empty" in error_msg:
            return {
                "status": "error",
                "message": "Password is required for this authentication method",
                "error": error_msg,
                "suggestion": (
                    "For native Okta authentication, you must provide SNOWFLAKE_PASSWORD. "
                    "Alternatively, use SNOWFLAKE_AUTHENTICATOR='externalbrowser' for browser-based SSO."
                )
            }
        elif "authentication failed" in error_msg.lower():
            return {
                "status": "error",
                "message": "Authentication failed",
                "error": error_msg,
                "suggestion": (
                    "This may be due to MFA requirements. "
                    "Ensure you're using SNOWFLAKE_AUTHENTICATOR='externalbrowser' "
                    "and that you can access a web browser from this environment."
                )
            }
        elif "Network policy" in error_msg or "network policy" in error_msg:
            return {
                "status": "error",
                "message": "Network policy restriction",
                "error": error_msg,
                "suggestion": (
                    "Your Snowflake account has network policy restrictions. "
                    "You may need to connect from an allowed IP address or "
                    "use browser-based authentication to bypass some restrictions."
                )
            }
        else:
            return {
                "status": "error",
                "message": "Authentication refresh failed",
                "error": error_msg,
                "suggestion": (
                    "Please check your Snowflake credentials and network connectivity. "
                    f"Current authenticator: {config.authenticator}"
                )
            }
