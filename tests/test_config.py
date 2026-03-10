"""Tests for server/config.py module."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from server.config import Config


class TestConfig:
    """Test Config class functionality."""

    def test_config_creation_with_required_fields(self):
        """Test creating a config with all required fields."""
        config = Config(
            account="test123.us-east-1",
            username="testuser",
            warehouse="TEST_WH",
            role="ANALYST",
        )

        assert config.account == "test123.us-east-1"
        assert config.username == "testuser"
        assert config.warehouse == "TEST_WH"
        assert config.role == "ANALYST"

    def test_config_creation_with_all_fields(self):
        """Test creating a config with all fields specified."""
        config = Config(
            account="test123.us-east-1",
            username="testuser",
            warehouse="TEST_WH",
            role="CUSTOM_ROLE",
            host="127.0.0.1",
            port=9000,
            transport="http",
            cache_ttl_days=10,
            max_query_rows=500,
            debug=True,
        )

        assert config.account == "test123.us-east-1"
        assert config.username == "testuser"
        assert config.warehouse == "TEST_WH"
        assert config.role == "CUSTOM_ROLE"
        assert config.host == "127.0.0.1"
        assert config.port == 9000
        assert config.transport == "http"
        assert config.cache_ttl_days == 10
        assert config.max_query_rows == 500
        assert config.debug is True

    def test_config_default_values(self):
        """Test that default values are set correctly."""
        config = Config(
            account="test123.us-east-1",
            username="testuser",
            warehouse="TEST_WH",
            role="ANALYST",
        )

        assert config.host == "0.0.0.0"
        assert config.port == 8000
        assert config.transport == "stdio"
        assert config.cache_ttl_days == 5
        assert config.max_query_rows == 100
        assert config.debug is False

    def test_account_validator_empty(self):
        """Test account validator rejects empty string."""
        with pytest.raises(ValidationError) as exc_info:
            Config(account="", username="testuser", warehouse="TEST_WH", role="ANALYST")

        assert "Snowflake account is required" in str(exc_info.value)

    def test_account_validator_invalid_characters(self):
        """Test account validator rejects invalid characters."""
        with pytest.raises(ValidationError) as exc_info:
            Config(
                account="test@123#invalid",
                username="testuser",
                warehouse="TEST_WH",
                role="ANALYST",
            )

        assert "Invalid Snowflake account format" in str(exc_info.value)

    def test_account_validator_valid_formats(self):
        """Test account validator accepts valid formats."""
        valid_accounts = [
            "test123",
            "test-123",
            "test.123",
            "test_123",
            "xy12345.us-east-1",
            "abc-def.ghi_123",
        ]

        for account in valid_accounts:
            config = Config(
                account=account,
                username="testuser",
                warehouse="TEST_WH",
                role="ANALYST",
            )
            assert config.account == account

    def test_transport_validator_invalid(self):
        """Test transport validator rejects invalid values."""
        with pytest.raises(ValidationError) as exc_info:
            Config(
                account="test123",
                username="testuser",
                warehouse="TEST_WH",
                role="ANALYST",
                transport="websocket",
            )

        assert "Transport must be 'stdio' or 'http'" in str(exc_info.value)

    def test_transport_validator_valid(self):
        """Test transport validator accepts valid values."""
        for transport in ["stdio", "http"]:
            config = Config(
                account="test123",
                username="testuser",
                warehouse="TEST_WH",
                role="ANALYST",
                transport=transport,
            )
            assert config.transport == transport

    def test_port_validation(self):
        """Test port number validation."""
        # Valid ports
        for port in [1, 80, 8080, 65535]:
            config = Config(
                account="test123",
                username="testuser",
                warehouse="TEST_WH",
                role="ANALYST",
                port=port,
            )
            assert config.port == port

        # Invalid ports
        for port in [0, -1, 65536, 100000]:
            with pytest.raises(ValidationError):
                Config(
                    account="test123",
                    username="testuser",
                    warehouse="TEST_WH",
                    role="ANALYST",
                    port=port,
                )

    @patch.dict(os.environ, {}, clear=True)
    def test_is_running_in_docker(self):
        """Test Docker detection."""
        config = Config(
            account="test123", username="testuser", warehouse="TEST_WH", role="ANALYST"
        )

        # Not in Docker by default
        with patch("os.path.exists", return_value=False):
            assert config.is_running_in_docker is False

        # Detect via .dockerenv file
        with patch("os.path.exists", return_value=True):
            assert config.is_running_in_docker is True

        # Detect via environment variable
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {"DOCKER_CONTAINER": "true"}),
        ):
            assert config.is_running_in_docker is True

    @patch.dict(os.environ, {}, clear=True)
    def test_is_running_in_docker_or_k8s(self):
        """Test Docker/Kubernetes detection."""
        config = Config(
            account="test123", username="testuser", warehouse="TEST_WH", role="ANALYST"
        )

        # Not in Docker/K8s by default
        with patch("os.path.exists", return_value=False):
            assert config.is_running_in_docker_or_k8s is False

        # Detect via Docker
        with patch("os.path.exists", return_value=True):
            assert config.is_running_in_docker_or_k8s is True

        # Detect via Kubernetes
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}),
        ):
            assert config.is_running_in_docker_or_k8s is True

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "test123.us-east-1",
            "SNOWFLAKE_USERNAME": "testuser",
            "SNOWFLAKE_WAREHOUSE": "TEST_WH",
            "SNOWFLAKE_ROLE": "ANALYST",
        },
        clear=True,
    )
    def test_from_env_with_required_vars(self):
        """Test creating config from environment with required variables."""
        config = Config.from_env()

        assert config.account == "test123.us-east-1"
        assert config.username == "testuser"
        assert config.warehouse == "TEST_WH"
        assert config.role == "ANALYST"

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "test123.us-east-1",
            "SNOWFLAKE_USERNAME": "testuser",
            "SNOWFLAKE_WAREHOUSE": "TEST_WH",
            "SNOWFLAKE_ROLE": "CUSTOM_ROLE",
            "MCP_HOST": "localhost",
            "MCP_PORT": "9000",
            "MCP_TRANSPORT": "http",
            "CACHE_TTL_DAYS": "10",
            "MAX_QUERY_ROWS": "500",
            "DEBUG": "true",
        },
        clear=True,
    )
    def test_from_env_with_all_vars(self):
        """Test creating config from environment with all variables."""
        config = Config.from_env()

        assert config.account == "test123.us-east-1"
        assert config.username == "testuser"
        assert config.warehouse == "TEST_WH"
        assert config.role == "CUSTOM_ROLE"
        assert config.host == "localhost"
        assert config.port == 9000
        assert config.transport == "http"
        assert config.cache_ttl_days == 10
        assert config.max_query_rows == 500
        assert config.debug is True

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_missing_required_vars(self):
        """Test from_env raises error when required variables are missing."""
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        error_msg = str(exc_info.value)
        assert "Missing required environment variables" in error_msg
        assert "SNOWFLAKE_ACCOUNT" in error_msg
        assert "SNOWFLAKE_USERNAME" in error_msg
        assert "SNOWFLAKE_WAREHOUSE" in error_msg
        assert "SNOWFLAKE_ROLE" in error_msg

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "test123",
            "SNOWFLAKE_USERNAME": "testuser",
            "SNOWFLAKE_ROLE": "ANALYST",
            # Missing SNOWFLAKE_WAREHOUSE
        },
        clear=True,
    )
    def test_from_env_partial_missing_vars(self):
        """Test from_env with partial missing variables."""
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        error_msg = str(exc_info.value)
        assert "Missing required environment variables" in error_msg
        assert "SNOWFLAKE_WAREHOUSE" in error_msg
        assert "SNOWFLAKE_ACCOUNT" not in error_msg  # This one is present
        assert "SNOWFLAKE_ROLE" not in error_msg  # This one is present

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "test123.us-east-1",
            "SNOWFLAKE_USERNAME": "testuser",
            "SNOWFLAKE_WAREHOUSE": "TEST_WH",
        },
        clear=True,
    )
    def test_from_env_missing_role(self):
        """Test from_env raises error when SNOWFLAKE_ROLE is missing."""
        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        error_msg = str(exc_info.value)
        assert "Missing required environment variables" in error_msg
        assert "SNOWFLAKE_ROLE" in error_msg
        assert "SNOWFLAKE_ACCOUNT" not in error_msg

    def test_config_missing_role_field(self):
        """Test Config raises validation error when role is not provided."""
        with pytest.raises(ValidationError):
            Config(account="test123", username="testuser", warehouse="TEST_WH")

    @patch.dict(
        os.environ,
        {
            "SNOWFLAKE_ACCOUNT": "test123.us-east-1",
            "SNOWFLAKE_USERNAME": "testuser",
            "SNOWFLAKE_WAREHOUSE": "TEST_WH",
            "SNOWFLAKE_ROLE": "ANALYST",
            "DEBUG": "yes",
        },
        clear=True,
    )
    def test_from_env_debug_parsing(self):
        """Test debug flag parsing from environment."""
        config = Config.from_env()
        assert config.debug is True

        # Test other true values
        for val in ["1", "true", "True", "TRUE", "yes", "YES"]:
            with patch.dict(os.environ, {"DEBUG": val}):
                config = Config.from_env()
                assert config.debug is True

        # Test false values
        for val in ["0", "false", "False", "no", "NO", ""]:
            with patch.dict(os.environ, {"DEBUG": val}):
                config = Config.from_env()
                assert config.debug is False
