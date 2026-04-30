"""
Extended tests for client creation functions to increase coverage.
"""

from unittest.mock import Mock, patch

import pytest

from berdl_notebook_utils.berdl_settings import BERDLSettings, get_settings
from berdl_notebook_utils.clients import (
    _get_hive_metastore_pool_cached,
    get_governance_client,
    get_hive_metastore_client,
    get_hive_metastore_pool,
    get_minio_client,
    get_s3_client,
    get_spark_cluster_client,
    get_task_service_client,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear LRU caches before each test."""
    get_task_service_client.cache_clear()
    get_minio_client.cache_clear()
    get_s3_client.cache_clear()
    get_governance_client.cache_clear()
    get_spark_cluster_client.cache_clear()
    _get_hive_metastore_pool_cached.cache_clear()
    get_settings.cache_clear()
    yield


class TestGetS3Client:
    """Tests for get_s3_client function."""

    def test_get_s3_client_creates_client(self):
        """Test that get_s3_client creates a Minio client with correct settings."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_minio.return_value = Mock()
            get_s3_client()

            mock_minio.assert_called_once()
            call_kwargs = mock_minio.call_args[1]
            assert "endpoint" in call_kwargs
            assert "access_key" in call_kwargs
            assert "secret_key" in call_kwargs
            assert "secure" in call_kwargs

    def test_get_s3_client_strips_protocol(self):
        """Test that http/https prefixes are stripped from endpoint."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_minio.return_value = Mock()
            get_s3_client()

            call_kwargs = mock_minio.call_args[1]
            # Endpoint should not contain http:// or https://
            assert not call_kwargs["endpoint"].startswith("http://")
            assert not call_kwargs["endpoint"].startswith("https://")

    def test_get_s3_client_caches_result(self):
        """Test that the client is cached on subsequent calls."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_client = Mock()
            mock_minio.return_value = mock_client

            client1 = get_s3_client()
            client2 = get_s3_client()

            # Should only be called once due to caching
            assert mock_minio.call_count == 1
            assert client1 is client2

    def test_get_minio_client_aliases_get_s3_client(self):
        """Test backward-compatible alias shares the same cached client."""
        with patch("berdl_notebook_utils.clients.Minio") as mock_minio:
            mock_client = Mock()
            mock_minio.return_value = mock_client

            client1 = get_s3_client()
            client2 = get_minio_client()

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

    def test_get_governance_client_refreshes_token_before_cache_lookup(self, tmp_path, monkeypatch):
        """Test that cached governance client is rebuilt after token cache changes."""
        token_file = tmp_path / ".berdl_kbase_session"
        monkeypatch.setattr(
            "berdl_notebook_utils.cache._get_token_cache_path",
            lambda: token_file,
        )
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "old-token")
        token_file.write_text("old-token")

        with patch("berdl_notebook_utils.clients.GovernanceAuthenticatedClient") as mock_client_class:
            old_client = Mock(name="old_client")
            new_client = Mock(name="new_client")
            mock_client_class.side_effect = [old_client, new_client]

            client1 = get_governance_client()

            token_file.write_text("new-token")
            client2 = get_governance_client()

            assert client1 is old_client
            assert client2 is new_client
            assert mock_client_class.call_args_list[0][1]["token"] == "old-token"
            assert mock_client_class.call_args_list[1][1]["token"] == "new-token"


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


class TestGetHiveMetastorePool:
    """Tests for the new pool-based HMS access."""

    def test_pool_is_built_from_thrift_uri(self):
        """``get_hive_metastore_pool`` parses the thrift:// URI from settings."""
        pool = get_hive_metastore_pool()
        # Settings come from conftest: thrift://localhost:9083.
        assert pool._host == "localhost"  # noqa: SLF001 - test of internals
        assert pool._port == 9083  # noqa: SLF001
        assert pool.max_size >= 1

    def test_pool_is_cached_per_process(self):
        """Repeat calls return the same pool instance (no per-call rebuild)."""
        assert get_hive_metastore_pool() is get_hive_metastore_pool()


class TestDeprecatedGetHiveMetastoreClient:
    """The legacy singleton accessor is preserved for transitional callers,
    but it MUST emit a DeprecationWarning so callers find and migrate it."""

    def test_emits_deprecation_warning(self):
        with patch.object(
            type(get_hive_metastore_pool()),
            "_new_client",
            return_value=Mock(),
        ):
            with pytest.warns(DeprecationWarning, match="thread-safe"):
                get_hive_metastore_client()


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
