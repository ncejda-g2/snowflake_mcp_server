"""Tests for server/log_utils.py module."""

import logging
import sys
import os
from unittest.mock import Mock, patch, MagicMock

import pytest

# Set required environment variables before importing to prevent module-level initialization errors
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test123.us-east-1")
os.environ.setdefault("SNOWFLAKE_USERNAME", "testuser")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "TEST_WH")

from server.log_utils import setup_logging


@pytest.fixture
def mock_config():
    """Create a mock config object."""
    config = Mock()
    config.debug = False
    return config


@pytest.fixture
def mock_config_debug():
    """Create a mock config object with debug enabled."""
    config = Mock()
    config.debug = True
    return config


def test_setup_logging_with_config(mock_config):
    """Test setup_logging with a provided config object."""
    with patch("logging.basicConfig") as mock_basic_config:
        logger = setup_logging(mock_config)

        # Verify basicConfig was called with correct parameters
        mock_basic_config.assert_called_once()
        call_args = mock_basic_config.call_args
        assert call_args[1]["level"] == logging.INFO
        assert "%(asctime)s" in call_args[1]["format"]
        assert len(call_args[1]["handlers"]) == 1
        assert isinstance(call_args[1]["handlers"][0], logging.StreamHandler)

        # Verify logger is returned
        assert isinstance(logger, logging.Logger)
        assert logger.name == "snowflake_mcp_server"


def test_setup_logging_with_debug_config(mock_config_debug):
    """Test setup_logging with debug enabled."""
    with patch("logging.basicConfig") as mock_basic_config:
        logger = setup_logging(mock_config_debug)

        # Verify debug level is set
        call_args = mock_basic_config.call_args
        assert call_args[1]["level"] == logging.DEBUG

        assert isinstance(logger, logging.Logger)


@patch("server.log_utils.Config")
def test_setup_logging_without_config(mock_config_class):
    """Test setup_logging without a provided config object."""
    mock_config_instance = Mock()
    mock_config_instance.debug = False
    mock_config_class.from_env.return_value = mock_config_instance

    with patch("logging.basicConfig"):
        logger = setup_logging(None)

        # Verify Config.from_env was called
        mock_config_class.from_env.assert_called_once()

        # Verify logger is returned
        assert isinstance(logger, logging.Logger)
        assert logger.name == "snowflake_mcp_server"


def test_setup_logging_adjusts_third_party_loggers(mock_config):
    """Test that third-party loggers are adjusted in non-debug mode."""
    with patch("logging.basicConfig"), \
         patch("logging.getLogger") as mock_get_logger:

        # Create mock loggers
        main_logger = Mock(spec=logging.Logger)
        snowflake_logger = Mock(spec=logging.Logger)
        urllib_logger = Mock(spec=logging.Logger)
        boto_logger = Mock(spec=logging.Logger)

        # Configure getLogger to return appropriate mocks
        def get_logger_side_effect(name):
            if name == "snowflake_mcp_server":
                return main_logger
            elif name == "snowflake.connector":
                return snowflake_logger
            elif name == "urllib3":
                return urllib_logger
            elif name == "botocore":
                return boto_logger
            return Mock(spec=logging.Logger)

        mock_get_logger.side_effect = get_logger_side_effect

        # Call setup_logging with debug=False
        logger = setup_logging(mock_config)

        # Verify third-party loggers were set to WARNING level
        snowflake_logger.setLevel.assert_called_once_with(logging.WARNING)
        urllib_logger.setLevel.assert_called_once_with(logging.WARNING)
        boto_logger.setLevel.assert_called_once_with(logging.WARNING)

        assert logger == main_logger


def test_setup_logging_does_not_adjust_loggers_in_debug(mock_config_debug):
    """Test that third-party loggers are not adjusted in debug mode."""
    with patch("logging.basicConfig"), \
         patch("logging.getLogger") as mock_get_logger:

        # Create mock loggers
        main_logger = Mock(spec=logging.Logger)
        snowflake_logger = Mock(spec=logging.Logger)
        urllib_logger = Mock(spec=logging.Logger)
        boto_logger = Mock(spec=logging.Logger)

        # Configure getLogger to return appropriate mocks
        def get_logger_side_effect(name):
            if name == "snowflake_mcp_server":
                return main_logger
            elif name == "snowflake.connector":
                return snowflake_logger
            elif name == "urllib3":
                return urllib_logger
            elif name == "botocore":
                return boto_logger
            return Mock(spec=logging.Logger)

        mock_get_logger.side_effect = get_logger_side_effect

        # Call setup_logging with debug=True
        logger = setup_logging(mock_config_debug)

        # Verify third-party loggers were NOT adjusted
        snowflake_logger.setLevel.assert_not_called()
        urllib_logger.setLevel.assert_not_called()
        boto_logger.setLevel.assert_not_called()

        assert logger == main_logger


def test_module_level_logger():
    """Test that module-level logger is created."""
    # Import the module to trigger module-level code
    import server.log_utils

    # The module should have a logger attribute
    assert hasattr(server.log_utils, 'logger')
    assert isinstance(server.log_utils.logger, logging.Logger)