"""
Tests for mcp/client.py - datalake MCP client factory.
"""

from unittest.mock import Mock, patch
import pytest

from berdl_notebook_utils.mcp.client import get_datalake_mcp_client, DEFAULT_TIMEOUT


@pytest.fixture(autouse=True)
def clear_cache():
    """Clear LRU cache before each test."""
    get_datalake_mcp_client.cache_clear()
    yield


class TestGetDatalakeMcpClient:
    """Tests for get_datalake_mcp_client function."""

    def test_creates_authenticated_client(self):
        """Test that the function creates an AuthenticatedClient with correct params."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            result = get_datalake_mcp_client()

            mock_client_class.assert_called_once()
            assert result is mock_client

    def test_uses_settings_for_base_url(self):
        """Test that client uses DATALAKE_MCP_SERVER_URL from settings."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()

            get_datalake_mcp_client()

            call_kwargs = mock_client_class.call_args[1]
            # URL may have trailing slash added by httpx/AuthenticatedClient
            assert call_kwargs["base_url"].rstrip("/") == "http://localhost:8080"

    def test_uses_settings_for_token(self):
        """Test that client uses KBASE_AUTH_TOKEN from settings."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()

            get_datalake_mcp_client()

            call_kwargs = mock_client_class.call_args[1]
            assert call_kwargs["token"] == "test-token-123"

    def test_sets_timeout(self):
        """Test that client is configured with the default timeout."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            with patch("berdl_notebook_utils.mcp.client.httpx.Timeout") as mock_timeout:
                mock_timeout.return_value = "timeout_object"
                mock_client_class.return_value = Mock()

                get_datalake_mcp_client()

                mock_timeout.assert_called_once_with(DEFAULT_TIMEOUT)
                call_kwargs = mock_client_class.call_args[1]
                assert call_kwargs["timeout"] == "timeout_object"

    def test_enables_ssl_verification(self):
        """Test that SSL verification is enabled."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()

            get_datalake_mcp_client()

            call_kwargs = mock_client_class.call_args[1]
            assert call_kwargs["verify_ssl"] is True

    def test_caches_client(self):
        """Test that the client is cached on subsequent calls."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            mock_client = Mock()
            mock_client_class.return_value = mock_client

            client1 = get_datalake_mcp_client()
            client2 = get_datalake_mcp_client()

            # Should only create the client once
            assert mock_client_class.call_count == 1
            assert client1 is client2

    def test_logs_info_message(self):
        """Test that an info message is logged during client creation."""
        with patch("berdl_notebook_utils.mcp.client.AuthenticatedClient") as mock_client_class:
            with patch("berdl_notebook_utils.mcp.client.logger") as mock_logger:
                mock_client_class.return_value = Mock()

                get_datalake_mcp_client()

                # Check that info was logged
                mock_logger.info.assert_called()
                log_message = mock_logger.info.call_args[0][0]
                assert "Creating datalake MCP client" in log_message

    def test_default_timeout_value(self):
        """Test that DEFAULT_TIMEOUT is set to 5 minutes (300 seconds)."""
        assert DEFAULT_TIMEOUT == 300.0
