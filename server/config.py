"""Configuration management for Snowflake MCP Server."""

import os

from pydantic import BaseModel, Field, field_validator


class Config(BaseModel):
    """Configuration settings for the Snowflake MCP Server."""

    # Snowflake connection (required from user)
    account: str = Field(
        description="Snowflake account identifier (e.g., xy12345.us-east-1)"
    )
    username: str = Field(description="Snowflake username")
    warehouse: str = Field(description="Compute warehouse to use")
    role: str = Field(description="Snowflake role to use", default="ML_DEVELOPER")

    # MCP server settings
    host: str = Field(default="0.0.0.0", description="Host for HTTP transport")
    port: int = Field(
        default=8000, ge=1, le=65535, description="Port for HTTP transport"
    )
    transport: str = Field(
        default="stdio", description="Transport protocol (stdio or http)"
    )

    # Cache settings
    cache_ttl_days: int = Field(default=5, description="Schema cache TTL in days")
    max_query_rows: int = Field(default=100, description="Max rows per query page")

    # Safety settings - ALWAYS enforce read-only, NEVER make this configurable
    debug: bool = Field(default=False, description="Enable debug logging")

    @property
    def is_running_in_docker(self) -> bool:
        """Check if the application is running inside Docker"""
        return os.path.exists("/.dockerenv") or os.getenv("DOCKER_CONTAINER") == "true"

    @property
    def is_running_in_docker_or_k8s(self) -> bool:
        """Check if the application is running inside Docker or k8s"""
        return self.is_running_in_docker or bool(
            os.environ.get("KUBERNETES_SERVICE_HOST")
        )

    @field_validator("account")
    @classmethod
    def validate_account(cls, v: str) -> str:
        """Validate Snowflake account format"""
        if not v:
            raise ValueError("Snowflake account is required")
        # Basic validation - account should contain alphanumeric and possibly dots/hyphens
        if not v.replace("-", "").replace(".", "").replace("_", "").isalnum():
            raise ValueError("Invalid Snowflake account format")
        return v

    @field_validator("transport")
    @classmethod
    def validate_transport(cls, v: str) -> str:
        """Validate transport type"""
        if v not in ("stdio", "http"):
            raise ValueError("Transport must be 'stdio' or 'http'")
        return v

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        # Check for required environment variables
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        username = os.getenv("SNOWFLAKE_USERNAME")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        role = os.getenv("SNOWFLAKE_ROLE", "ML_DEVELOPER")

        if not all([account, username, warehouse]):
            missing = []
            if not account:
                missing.append("SNOWFLAKE_ACCOUNT")
            if not username:
                missing.append("SNOWFLAKE_USERNAME")
            if not warehouse:
                missing.append("SNOWFLAKE_WAREHOUSE")
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        # After validation, these cannot be None - help mypy understand
        if account is None or username is None or warehouse is None:
            raise ValueError("Required environment variables are not set")

        return cls(
            account=account,
            username=username,
            warehouse=warehouse,
            role=role,
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=int(os.getenv("MCP_PORT", "8000")),
            transport=os.getenv("MCP_TRANSPORT", "stdio"),
            cache_ttl_days=int(os.getenv("CACHE_TTL_DAYS", "5")),
            max_query_rows=int(os.getenv("MAX_QUERY_ROWS", "100")),
            debug=os.getenv("DEBUG", "false").lower() in ("1", "true", "yes"),
        )
