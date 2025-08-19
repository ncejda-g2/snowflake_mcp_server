"""Configuration management for Snowflake MCP Server."""

import os

from pydantic import BaseModel, Field, field_validator


class Config(BaseModel):
    """Configuration settings for the Snowflake MCP Server."""

    host: str = Field(default="0.0.0.0")
    port: int = Field(
        default=8000, ge=1, le=65535, description="Port for the server to listen on"
    )
    transport: str = Field(
        default="http", description="Transport protocol for the server"
    )
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

    @field_validator("base_url")
    @classmethod
    def validate_base_url(cls, v):
        if not v.startswith(("http://", "https://")):
            raise ValueError("Base URL must start with http:// or https://")
        return v.rstrip("/")

    @classmethod
    def from_env(cls) -> "Config":
        """Create configuration from environment variables."""
        return cls(
            host=os.getenv("MCP_HOST", "0.0.0.0"),
            port=int(os.getenv("MCP_PORT", "8000")),
            transport=os.getenv("MCP_TRANSPORT", "http"),
            debug=os.getenv("DEBUG", "1").lower() in ("1", "true", "yes"),
        )
