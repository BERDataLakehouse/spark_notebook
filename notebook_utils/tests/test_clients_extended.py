"""
Extended tests for client creation functions to increase coverage.
"""

from unittest.mock import Mock, patch

import pytest

from berdl_notebook_utils.berdl_settings import BERDLSettings
from berdl_notebook_utils.clients import (
    get_task_service_client,
    get_minio_client,
    get_governance_client,
    get_spark_cluster_client,
    get_hive_metastore_client,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear LRU caches before each test."""
    get_task_service_client.cache_clear()
    get_minio_client.cache_clear()
    get_governance_client.cache_clear()
    get_spark_cluster_client.cache_clear()
    get_hive_metastore_client.cache_clear()
    yield


class TestGetMinioClient:
    """Tests for get_minio_client function."""

    def test_get_minio_client_creates_client(self):
        """Test that get_minio_client creates a Minio client with correct settings."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_minio.return_value = Mock()
            get_minio_client()

            mock_minio.assert_called_once()
            call_kwargs = mock_minio.call_args[1]
            assert "endpoint" in call_kwargs
            assert "access_key" in call_kwargs
            assert "secret_key" in call_kwargs
            assert "secure" in call_kwargs

    def test_get_minio_client_strips_protocol(self):
        """Test that http/https prefixes are stripped from endpoint."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_minio.return_value = Mock()
            get_minio_client()

            call_kwargs = mock_minio.call_args[1]
            # Endpoint should not contain http:// or https://
            assert not call_kwargs["endpoint"].startswith("http://")
            assert not call_kwargs["endpoint"].startswith("https://")

    def test_get_minio_client_caches_result(self):
        """Test that the client is cached on subsequent calls."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_client = Mock()
            mock_minio.return_value = mock_client

            client1 = get_minio_client()
            client2 = get_minio_client()

            # Should only be called once due to caching
            assert mock_minio.call_count == 1
            assert client1 is client2


class TestGetGovernanceClient:
    """Tests for get_governance_client function."""

    def test_get_governance_client_creates_client(self):
        """Test that get_governance_client creates an authenticated client."""
        with patch("berdl_notebook_utils.clients.GovernanceAuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()
            get_governance_client()

            mock_client_class.assert_called_once()
            call_kwargs = mock_client_class.call_args[1]
            assert "base_url" in call_kwargs
            assert "token" in call_kwargs

    def test_get_governance_client_uses_settings(self):
        """Test that client uses GOVERNANCE_API_URL from settings."""
        with patch("berdl_notebook_utils.clients.GovernanceAuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()
            get_governance_client()

            call_kwargs = mock_client_class.call_args[1]
            # URL may have trailing slash added by httpx/AuthenticatedClient
            assert call_kwargs["base_url"].rstrip("/") == "http://localhost:8000"
            assert call_kwargs["token"] == "test-token-123"


class TestGetSparkClusterClient:
    """Tests for get_spark_cluster_client function."""

    def test_get_spark_cluster_client_creates_client(self):
        """Test that get_spark_cluster_client creates an authenticated client."""
        with patch("berdl_notebook_utils.clients.SparkAuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()
            get_spark_cluster_client()

            mock_client_class.assert_called_once()
            call_kwargs = mock_client_class.call_args[1]
            assert "base_url" in call_kwargs
            assert "token" in call_kwargs

    def test_get_spark_cluster_client_uses_settings(self):
        """Test that client uses SPARK_CLUSTER_MANAGER_API_URL from settings."""
        with patch("berdl_notebook_utils.clients.SparkAuthenticatedClient") as mock_client_class:
            mock_client_class.return_value = Mock()
            get_spark_cluster_client()

            call_kwargs = mock_client_class.call_args[1]
            # URL may have trailing slash added by httpx/AuthenticatedClient
            assert call_kwargs["base_url"].rstrip("/") == "http://localhost:8000"


class TestGetHiveMetastoreClient:
    """Tests for get_hive_metastore_client function."""

    def test_get_hive_metastore_client_creates_client(self):
        """Test that get_hive_metastore_client creates an HMS client."""
        with patch("berdl_notebook_utils.clients.HMSClient") as mock_hms:
            mock_hms.return_value = Mock()
            get_hive_metastore_client()

            mock_hms.assert_called_once()
            call_kwargs = mock_hms.call_args[1]
            assert "host" in call_kwargs
            assert "port" in call_kwargs

    def test_get_hive_metastore_client_parses_thrift_uri(self):
        """Test that the thrift URI is correctly parsed."""
        with patch("berdl_notebook_utils.clients.HMSClient") as mock_hms:
            mock_hms.return_value = Mock()
            get_hive_metastore_client()

            call_kwargs = mock_hms.call_args[1]
            assert call_kwargs["host"] == "localhost"
            assert call_kwargs["port"] == 9083


class TestClientWithCustomSettings:
    """Tests for client functions with custom settings."""

    def test_get_task_service_client_with_custom_settings(self):
        """Test that custom settings can be passed."""
        with patch("berdl_notebook_utils.clients.CTSClient") as mock_cts:
            mock_cts.return_value = Mock()
            mock_cts.return_value._test_cts_connection = Mock()

            # Create mock settings
            mock_settings = Mock(spec=BERDLSettings)
            mock_settings.KBASE_AUTH_TOKEN = "custom-token"
            mock_settings.CDM_TASK_SERVICE_URL = "http://custom-url:8080"

            get_task_service_client(settings=mock_settings)

            mock_cts.assert_called_once_with("custom-token", url="http://custom-url:8080")
