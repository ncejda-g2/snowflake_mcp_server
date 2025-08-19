"""Tests for authentication utilities."""

from unittest.mock import patch

import httpx

from server.tools.auth_utils import (
    create_base_client,
    get_auth_headers,
    merge_headers,
)


class TestCreateBaseClient:
    """Test cases for create_base_client function."""

    def test_create_base_client_defaults(self):
        """Test creating base client with default values."""
        client = create_base_client()

        assert isinstance(client, httpx.AsyncClient)
        assert str(client.base_url) == ""
        assert client.timeout.read is None
        # Check that no custom headers are set (httpx has default headers)
        assert "Content-Type" not in client.headers
        assert "Authorization" not in client.headers

    def test_create_base_client_with_base_url(self):
        """Test creating base client with base URL."""
        base_url = "https://api.example.com"
        client = create_base_client(base_url=base_url)

        assert str(client.base_url) == base_url

    def test_create_base_client_with_timeout(self):
        """Test creating base client with timeout."""
        timeout = 60.0
        client = create_base_client(base_url="https://api.example.com", timeout=timeout)

        assert client.timeout.read == timeout

    def test_create_base_client_with_additional_headers(self):
        """Test creating base client with additional headers."""
        headers = {"Content-Type": "application/json", "X-Custom": "value"}
        client = create_base_client(
            base_url="https://api.example.com", additional_headers=headers
        )

        for key, value in headers.items():
            assert client.headers[key] == value

    @patch("server.tools.auth_utils.logging")
    def test_create_base_client_with_debug_logging(self, mock_logging):
        """Test creating base client with debug logging enabled."""
        headers = {"Content-Type": "application/json"}
        create_base_client(
            base_url="https://api.example.com",
            additional_headers=headers,
            debug_logging=True,
        )

        mock_logging.debug.assert_called_once_with(
            f"Base client initialized with headers: {headers}"
        )

    def test_create_base_client_with_kwargs(self):
        """Test creating base client with additional kwargs."""
        client = create_base_client(
            base_url="https://api.example.com", follow_redirects=True
        )

        assert client.follow_redirects is True


class TestGetAuthHeaders:
    """Test cases for get_auth_headers function."""

    @patch("server.tools.auth_utils.get_http_headers")
    def test_get_auth_headers_with_authorization_header(self, mock_get_headers):
        """Test getting auth headers when authorization header is present."""
        mock_get_headers.return_value = {"authorization": "Bearer token123"}

        result = get_auth_headers()

        assert result == {"Authorization": "Bearer token123"}

    @patch("server.tools.auth_utils.get_http_headers")
    def test_get_auth_headers_with_capitalized_authorization(self, mock_get_headers):
        """Test getting auth headers with capitalized Authorization header."""
        mock_get_headers.return_value = {"Authorization": "Bearer token123"}

        result = get_auth_headers()

        assert result == {"Authorization": "Bearer token123"}

    @patch("server.tools.auth_utils.get_http_headers")
    def test_get_auth_headers_prefers_lowercase(self, mock_get_headers):
        """Test that lowercase 'authorization' takes precedence."""
        mock_get_headers.return_value = {
            "authorization": "Bearer token123",
            "Authorization": "Bearer token456",
        }

        result = get_auth_headers()

        assert result == {"Authorization": "Bearer token123"}

    @patch("server.tools.auth_utils.get_http_headers")
    def test_get_auth_headers_no_mcp_headers(self, mock_get_headers):
        """Test getting auth headers when no MCP headers are present."""
        mock_get_headers.return_value = None

        result = get_auth_headers()

        assert result == {}

    @patch("server.tools.auth_utils.get_http_headers")
    def test_get_auth_headers_no_auth_header(self, mock_get_headers):
        """Test getting auth headers when no authorization header is present."""
        mock_get_headers.return_value = {"Content-Type": "application/json"}

        result = get_auth_headers()

        assert result == {}

    @patch("server.tools.auth_utils.get_http_headers")
    @patch("server.tools.auth_utils.logging")
    def test_get_auth_headers_with_debug_logging(self, mock_logging, mock_get_headers):
        """Test getting auth headers with debug logging enabled."""
        mcp_headers = {"authorization": "Bearer token123"}
        mock_get_headers.return_value = mcp_headers

        result = get_auth_headers(debug_logging=True)

        expected_auth_headers = {"Authorization": "Bearer token123"}
        assert result == expected_auth_headers

        mock_logging.debug.assert_any_call(f"MCP headers received: {mcp_headers}")
        mock_logging.debug.assert_any_call(
            f"Auth headers extracted: {expected_auth_headers}"
        )

    @patch("server.tools.auth_utils.get_http_headers")
    @patch("server.tools.auth_utils.logging")
    def test_get_auth_headers_debug_logging_no_headers(
        self, mock_logging, mock_get_headers
    ):
        """Test debug logging when no headers are found."""
        mock_get_headers.return_value = None

        result = get_auth_headers(debug_logging=True)

        assert result == {}
        mock_logging.debug.assert_called_once_with(
            "No MCP headers found or no authorization header present"
        )


class TestMergeHeaders:
    """Test cases for merge_headers function."""

    def test_merge_headers_empty(self):
        """Test merging empty headers."""
        result = merge_headers()
        assert result == {}

    def test_merge_headers_single_dict(self):
        """Test merging single header dictionary."""
        headers = {"Content-Type": "application/json"}
        result = merge_headers(headers)
        assert result == headers

    def test_merge_headers_multiple_dicts(self):
        """Test merging multiple header dictionaries."""
        headers1 = {"Content-Type": "application/json", "X-Custom": "value1"}
        headers2 = {"Authorization": "Bearer token", "X-Custom": "value2"}

        result = merge_headers(headers1, headers2)

        expected = {
            "Content-Type": "application/json",
            "Authorization": "Bearer token",
            "X-Custom": "value2",  # Later dict takes precedence
        }
        assert result == expected

    def test_merge_headers_with_none_values(self):
        """Test merging headers with None values."""
        headers1 = {"Content-Type": "application/json"}
        headers2 = None
        headers3 = {"Authorization": "Bearer token"}

        result = merge_headers(headers1, headers2, headers3)

        expected = {"Content-Type": "application/json", "Authorization": "Bearer token"}
        assert result == expected

    def test_merge_headers_all_none(self):
        """Test merging when all header dicts are None."""
        result = merge_headers(None, None, None)
        assert result == {}
