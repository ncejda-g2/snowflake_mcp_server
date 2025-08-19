"""Tests for logging utilities."""

import logging
from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from server.log_utils import (
    async_log_request,
    async_log_response,
    log_request,
    log_response,
    setup_logging,
)


class TestSetupLogging:
    """Test cases for setup_logging function."""

    @patch("server.log_utils.config")
    @patch("server.log_utils.logging")
    def test_setup_logging_debug_mode(self, mock_logging, mock_config):
        """Test logging setup in debug mode."""
        mock_config.debug = True
        mock_logger = MagicMock()
        mock_logging.getLogger.return_value = mock_logger
        mock_logging.DEBUG = logging.DEBUG  # Use the actual logging constants

        result = setup_logging()

        mock_logging.basicConfig.assert_called_once_with(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        mock_logging.getLogger.assert_called_with("g2_mcp_server")
        assert result == mock_logger

    @patch("server.log_utils.config")
    @patch("server.log_utils.logging")
    def test_setup_logging_info_mode(self, mock_logging, mock_config):
        """Test logging setup in info mode."""
        mock_config.debug = False
        mock_logger = MagicMock()
        mock_logging.getLogger.return_value = mock_logger
        mock_logging.INFO = logging.INFO  # Use the actual logging constants

        result = setup_logging()

        mock_logging.basicConfig.assert_called_once_with(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        mock_logging.getLogger.assert_called_with("g2_mcp_server")
        assert result == mock_logger


class TestLogRequest:
    """Test cases for log_request function."""

    @patch("server.log_utils.logger")
    def test_log_request_basic(self, mock_logger):
        """Test basic request logging."""
        request = httpx.Request("GET", "https://api.example.com/test")

        log_request(request)

        mock_logger.info.assert_called_once_with(
            "Request: GET https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Request headers: {dict(request.headers)}"
        )

    @patch("server.log_utils.logger")
    def test_log_request_with_headers(self, mock_logger):
        """Test request logging with custom headers."""
        headers = {
            "Authorization": "Bearer token123",
            "Content-Type": "application/json",
        }
        request = httpx.Request("POST", "https://api.example.com/test", headers=headers)

        log_request(request)

        mock_logger.info.assert_called_once_with(
            "Request: POST https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Request headers: {dict(request.headers)}"
        )

    @patch("server.log_utils.logger")
    def test_log_request_with_content(self, mock_logger):
        """Test request logging with body content."""
        content = b'{"key": "value"}'
        request = httpx.Request("POST", "https://api.example.com/test", content=content)

        log_request(request)

        expected_calls = [
            call("Request: POST https://api.example.com/test"),
        ]
        mock_logger.info.assert_has_calls(expected_calls)

        expected_debug_calls = [
            call(f"Request headers: {dict(request.headers)}"),
            call('Request body: {"key": "value"}'),
        ]
        mock_logger.debug.assert_has_calls(expected_debug_calls)

    @patch("server.log_utils.logger")
    def test_log_request_with_invalid_utf8_content(self, mock_logger):
        """Test request logging with invalid UTF-8 content."""
        content = b"\xff\xfe\\invalid\\utf8"
        request = httpx.Request("POST", "https://api.example.com/test", content=content)

        log_request(request)

        mock_logger.info.assert_called_once_with(
            "Request: POST https://api.example.com/test"
        )

        # Should handle decode errors gracefully
        debug_calls = mock_logger.debug.call_args_list
        assert len(debug_calls) == 2
        assert "Request headers:" in debug_calls[0][0][0]
        assert "Request body:" in debug_calls[1][0][0]


class TestLogResponse:
    """Test cases for log_response function."""

    @patch("server.log_utils.logger")
    def test_log_response_basic(self, mock_logger):
        """Test basic response logging."""
        response = httpx.Response(
            200, request=httpx.Request("GET", "https://api.example.com/test")
        )

        log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 200 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )

    @patch("server.log_utils.logger")
    def test_log_response_with_error_status(self, mock_logger):
        """Test response logging with error status."""
        response = httpx.Response(
            404, request=httpx.Request("GET", "https://api.example.com/test")
        )

        log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 404 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )

    @patch("server.log_utils.logger")
    def test_log_response_with_headers(self, mock_logger):
        """Test response logging with custom headers."""
        headers = {"Content-Type": "application/json", "X-Rate-Limit": "100"}
        response = httpx.Response(
            200,
            headers=headers,
            request=httpx.Request("GET", "https://api.example.com/test"),
        )

        log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 200 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )


class TestAsyncLogRequest:
    """Test cases for async_log_request function."""

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_request_basic(self, mock_logger):
        """Test basic async request logging."""
        request = httpx.Request("GET", "https://api.example.com/test")

        await async_log_request(request)

        mock_logger.info.assert_called_once_with(
            "Request: GET https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Request headers: {dict(request.headers)}"
        )

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_request_with_content(self, mock_logger):
        """Test async request logging with body content."""
        content = b'{"key": "value"}'
        request = httpx.Request("POST", "https://api.example.com/test", content=content)

        await async_log_request(request)

        expected_calls = [
            call("Request: POST https://api.example.com/test"),
        ]
        mock_logger.info.assert_has_calls(expected_calls)

        expected_debug_calls = [
            call(f"Request headers: {dict(request.headers)}"),
            call('Request body: {"key": "value"}'),
        ]
        mock_logger.debug.assert_has_calls(expected_debug_calls)

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_request_with_invalid_utf8_content(self, mock_logger):
        """Test async request logging with invalid UTF-8 content."""
        content = b"\xff\xfe\\invalid\\utf8"
        request = httpx.Request("POST", "https://api.example.com/test", content=content)

        await async_log_request(request)

        mock_logger.info.assert_called_once_with(
            "Request: POST https://api.example.com/test"
        )

        # Should handle decode errors gracefully
        debug_calls = mock_logger.debug.call_args_list
        assert len(debug_calls) == 2
        assert "Request headers:" in debug_calls[0][0][0]
        assert "Request body:" in debug_calls[1][0][0]


class TestAsyncLogResponse:
    """Test cases for async_log_response function."""

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_response_basic(self, mock_logger):
        """Test basic async response logging."""
        response = httpx.Response(
            200, request=httpx.Request("GET", "https://api.example.com/test")
        )

        await async_log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 200 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_response_with_error_status(self, mock_logger):
        """Test async response logging with error status."""
        response = httpx.Response(
            500, request=httpx.Request("GET", "https://api.example.com/test")
        )

        await async_log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 500 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )

    @patch("server.log_utils.logger")
    @pytest.mark.asyncio
    async def test_async_log_response_with_headers(self, mock_logger):
        """Test async response logging with custom headers."""
        headers = {"Content-Type": "application/json", "X-Rate-Limit": "100"}
        response = httpx.Response(
            200,
            headers=headers,
            request=httpx.Request("GET", "https://api.example.com/test"),
        )

        await async_log_response(response)

        mock_logger.info.assert_called_once_with(
            "Response: 200 https://api.example.com/test"
        )
        mock_logger.debug.assert_called_once_with(
            f"Response headers: {dict(response.headers)}"
        )


class TestModuleInitialization:
    """Test cases for module-level initialization."""

    def test_module_config_initialization(self):
        """Test that module-level config is initialized correctly."""
        import server.log_utils

        # Test that config is initialized and has expected attributes
        assert server.log_utils.config is not None
        assert hasattr(server.log_utils.config, "debug")
        assert hasattr(server.log_utils.config, "base_url")
        assert hasattr(server.log_utils.config, "timeout")

    def test_module_logger_initialization(self):
        """Test that module-level logger is initialized correctly."""
        import server.log_utils

        # Should have a logger instance
        assert hasattr(server.log_utils, "logger")
        assert isinstance(server.log_utils.logger, logging.Logger)
        assert server.log_utils.logger.name == "server.log_utils"
