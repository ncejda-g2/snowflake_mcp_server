"""Tests for server/log_utils.py module."""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test123.us-east-1")
os.environ.setdefault("SNOWFLAKE_USERNAME", "testuser")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "TEST_WH")
os.environ.setdefault("SNOWFLAKE_ROLE", "ANALYST")

from server.log_utils import LOG_FILE_PATH, setup_logging


@pytest.fixture
def mock_config():
    config = Mock()
    config.debug = False
    return config


@pytest.fixture
def mock_config_debug():
    config = Mock()
    config.debug = True
    return config


def test_setup_logging_with_config(mock_config):
    with patch("logging.basicConfig") as mock_basic_config:
        logger = setup_logging(mock_config)

        mock_basic_config.assert_called_once()
        call_args = mock_basic_config.call_args
        assert call_args[1]["level"] == logging.INFO
        assert "%(asctime)s" in call_args[1]["format"]

        handlers = call_args[1]["handlers"]
        assert len(handlers) == 2
        assert isinstance(handlers[0], logging.StreamHandler)
        assert isinstance(handlers[1], RotatingFileHandler)

        assert isinstance(logger, logging.Logger)
        assert logger.name == "snowflake_mcp_server"


def test_setup_logging_with_debug_config(mock_config_debug):
    with patch("logging.basicConfig") as mock_basic_config:
        logger = setup_logging(mock_config_debug)

        call_args = mock_basic_config.call_args
        assert call_args[1]["level"] == logging.DEBUG

        assert isinstance(logger, logging.Logger)


@patch("server.log_utils.Config")
def test_setup_logging_without_config(mock_config_class):
    mock_config_instance = Mock()
    mock_config_instance.debug = False
    mock_config_class.from_env.return_value = mock_config_instance

    with patch("logging.basicConfig"):
        logger = setup_logging(None)

        mock_config_class.from_env.assert_called_once()

        assert isinstance(logger, logging.Logger)
        assert logger.name == "snowflake_mcp_server"


def test_setup_logging_adjusts_third_party_loggers(mock_config):
    with patch("logging.basicConfig"), patch("logging.getLogger") as mock_get_logger:
        main_logger = Mock(spec=logging.Logger)
        snowflake_logger = Mock(spec=logging.Logger)
        urllib_logger = Mock(spec=logging.Logger)
        boto_logger = Mock(spec=logging.Logger)

        logger_map = {
            "snowflake_mcp_server": main_logger,
            "snowflake.connector": snowflake_logger,
            "urllib3": urllib_logger,
            "botocore": boto_logger,
        }

        def get_logger_side_effect(name):
            return logger_map.get(name, Mock(spec=logging.Logger))

        mock_get_logger.side_effect = get_logger_side_effect

        logger = setup_logging(mock_config)

        snowflake_logger.setLevel.assert_called_once_with(logging.WARNING)
        urllib_logger.setLevel.assert_called_once_with(logging.WARNING)
        boto_logger.setLevel.assert_called_once_with(logging.WARNING)

        assert logger == main_logger


def test_setup_logging_does_not_adjust_loggers_in_debug(mock_config_debug):
    with patch("logging.basicConfig"), patch("logging.getLogger") as mock_get_logger:
        main_logger = Mock(spec=logging.Logger)
        snowflake_logger = Mock(spec=logging.Logger)
        urllib_logger = Mock(spec=logging.Logger)
        boto_logger = Mock(spec=logging.Logger)

        logger_map = {
            "snowflake_mcp_server": main_logger,
            "snowflake.connector": snowflake_logger,
            "urllib3": urllib_logger,
            "botocore": boto_logger,
        }

        def get_logger_side_effect(name):
            return logger_map.get(name, Mock(spec=logging.Logger))

        mock_get_logger.side_effect = get_logger_side_effect

        logger = setup_logging(mock_config_debug)

        snowflake_logger.setLevel.assert_not_called()
        urllib_logger.setLevel.assert_not_called()
        boto_logger.setLevel.assert_not_called()

        assert logger == main_logger


def test_module_level_logger():
    import server.log_utils

    assert hasattr(server.log_utils, "logger")
    assert isinstance(server.log_utils.logger, logging.Logger)


def test_log_file_path():
    assert Path.home() / ".snowflake_mcp" / "server.log" == LOG_FILE_PATH


def test_file_handler_writes_to_correct_path(mock_config):
    with patch("logging.basicConfig") as mock_basic_config:
        setup_logging(mock_config)

        call_args = mock_basic_config.call_args
        handlers = call_args[1]["handlers"]
        file_handler = [h for h in handlers if isinstance(h, RotatingFileHandler)]
        assert len(file_handler) == 1
        assert file_handler[0].baseFilename == str(LOG_FILE_PATH)


def test_file_handler_rotation_config(mock_config):
    with patch("logging.basicConfig") as mock_basic_config:
        setup_logging(mock_config)

        call_args = mock_basic_config.call_args
        handlers = call_args[1]["handlers"]
        file_handler = [h for h in handlers if isinstance(h, RotatingFileHandler)][0]
        assert file_handler.maxBytes == 5 * 1024 * 1024
        assert file_handler.backupCount == 2


@patch("server.log_utils._create_file_handler", return_value=None)
def test_graceful_fallback_when_file_handler_fails(_mock_create, mock_config):
    with patch("logging.basicConfig") as mock_basic_config:
        logger = setup_logging(mock_config)

        call_args = mock_basic_config.call_args
        handlers = call_args[1]["handlers"]
        assert len(handlers) == 1
        assert isinstance(handlers[0], logging.StreamHandler)

        assert isinstance(logger, logging.Logger)
