"""Tests for configuration management."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from server.config import Config


class TestConfig:
    """Test cases for Config class."""

    def test_default_config(self):
        """Test creating config with default values."""
        config = Config()
        assert config.base_url == "https://data.g2.com/api/v2"
        assert config.timeout == 30
        assert config.user_agent == "G2-MCP-Server/1.0"
        assert config.debug is False
        assert config.max_page_size == 100

    def test_custom_config(self):
        """Test creating config with custom values."""
        config = Config(
            base_url="https://custom.api.com/v2",
            timeout=60,
            user_agent="Custom-Agent/2.0",
            debug=True,
            max_page_size=50,
        )
        assert config.base_url == "https://custom.api.com/v2"
        assert config.timeout == 60
        assert config.user_agent == "Custom-Agent/2.0"
        assert config.debug is True
        assert config.max_page_size == 50

    def test_base_url_validation_strips_trailing_slash(self):
        """Test that trailing slash is stripped from base URL."""
        config = Config(base_url="https://api.com/v1/")
        assert config.base_url == "https://api.com/v1"

    def test_base_url_validation_requires_protocol(self):
        """Test that base URL must start with http:// or https://."""
        with pytest.raises(ValidationError, match="Base URL must start with"):
            Config(base_url="api.com")

        with pytest.raises(ValidationError, match="Base URL must start with"):
            Config(base_url="ftp://api.com")

    def test_timeout_validation(self):
        """Test timeout validation constraints."""
        # Valid timeouts
        Config(timeout=1)  # minimum
        Config(timeout=300)  # maximum
        Config(timeout=30)  # in between

        # Invalid timeouts
        with pytest.raises(ValidationError):
            Config(timeout=0)  # too low

        with pytest.raises(ValidationError):
            Config(timeout=301)  # too high

    def test_max_page_size_validation(self):
        """Test max page size validation constraints."""
        # Valid page sizes
        Config(max_page_size=1)  # minimum
        Config(max_page_size=100)  # maximum
        Config(max_page_size=50)  # in between

        # Invalid page sizes
        with pytest.raises(ValidationError):
            Config(max_page_size=0)  # too low

        with pytest.raises(ValidationError):
            Config(max_page_size=101)  # too high

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_defaults(self):
        """Test creating config from environment with no env vars set."""
        config = Config.from_env()
        assert config.base_url == "https://data.g2.com"
        assert config.timeout == 30
        assert config.user_agent == "G2-MCP-Server/1.0"
        assert config.debug is True
        assert config.max_page_size == 100

    @patch.dict(
        os.environ,
        {
            "G2_BASE_URL": "https://custom.api.com",
            "G2_TIMEOUT": "60",
            "G2_USER_AGENT": "Custom/1.0",
            "DEBUG": "true",
            "G2_MAX_PAGE_SIZE": "50",
        },
    )
    def test_from_env_custom_values(self):
        """Test creating config from environment with custom values."""
        config = Config.from_env()
        assert config.base_url == "https://custom.api.com"
        assert config.timeout == 60
        assert config.user_agent == "Custom/1.0"
        assert config.debug is True
        assert config.max_page_size == 50

    @patch.dict(os.environ, {"DEBUG": "1"})
    def test_from_env_debug_variations(self):
        """Test different debug environment variable values."""
        # Test various truthy values
        for debug_val in ["1", "true", "TRUE", "yes", "YES"]:
            with patch.dict(os.environ, {"DEBUG": debug_val}):
                config = Config.from_env()
                assert config.debug is True

        # Test falsy values
        for debug_val in ["0", "false", "FALSE", "no", "NO", ""]:
            with patch.dict(os.environ, {"DEBUG": debug_val}):
                config = Config.from_env()
                assert config.debug is False

    def test_is_running_in_docker_dockerenv_exists(self):
        """Test Docker detection when .dockerenv file exists."""
        config = Config()
        with patch("os.path.exists", return_value=True):
            assert config.is_running_in_docker is True

    def test_is_running_in_docker_env_var_set(self):
        """Test Docker detection when DOCKER_CONTAINER env var is set."""
        config = Config()
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {"DOCKER_CONTAINER": "true"}),
        ):
            assert config.is_running_in_docker is True

    def test_is_running_in_docker_false(self):
        """Test Docker detection when not in Docker."""
        config = Config()
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {}, clear=True),
        ):
            assert config.is_running_in_docker is False

    def test_is_running_in_docker_or_k8s_docker_true(self):
        """Test Docker/K8s detection when Docker is true."""
        config = Config()
        with (
            patch("os.path.exists", return_value=True),
            patch.dict(os.environ, {}, clear=True),
        ):
            assert config.is_running_in_docker_or_k8s is True

    def test_is_running_in_docker_or_k8s_k8s_true(self):
        """Test Docker/K8s detection when K8s env var is set."""
        config = Config()
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}),
        ):
            assert config.is_running_in_docker_or_k8s is True

    def test_is_running_in_docker_or_k8s_false(self):
        """Test Docker/K8s detection when neither is true."""
        config = Config()
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {}, clear=True),
        ):
            assert config.is_running_in_docker_or_k8s is False

    def test_environment_detection_integration(self):
        """Test environment detection properties work together."""
        config = Config()

        # Test Docker environment
        with (
            patch("os.path.exists", return_value=True),
            patch.dict(os.environ, {}, clear=True),
        ):
            assert config.is_running_in_docker is True
            assert config.is_running_in_docker_or_k8s is True

        # Test K8s environment (not Docker)
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {"KUBERNETES_SERVICE_HOST": "10.0.0.1"}, clear=True),
        ):
            assert config.is_running_in_docker is False
            assert config.is_running_in_docker_or_k8s is True

        # Test neither environment
        with (
            patch("os.path.exists", return_value=False),
            patch.dict(os.environ, {}, clear=True),
        ):
            assert config.is_running_in_docker is False
            assert config.is_running_in_docker_or_k8s is False
