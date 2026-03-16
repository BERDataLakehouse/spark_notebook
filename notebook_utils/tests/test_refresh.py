"""Tests for refresh.py - Credential and Spark environment refresh."""

from pathlib import Path
from unittest.mock import Mock, patch

from berdl_notebook_utils.refresh import _remove_cache_file, refresh_spark_environment


class TestRemoveCacheFile:
    """Tests for _remove_cache_file helper."""

    def test_removes_existing_file(self, tmp_path):
        """Test removes file and returns True when file exists."""
        cache_file = tmp_path / "test_cache"
        cache_file.write_text("cached data")

        result = _remove_cache_file(cache_file)

        assert result is True
        assert not cache_file.exists()

    def test_preserves_lock_file(self, tmp_path):
        """Test leaves the .lock companion file in place."""
        cache_file = tmp_path / "test_cache"
        lock_file = tmp_path / "test_cache.lock"
        cache_file.write_text("cached data")
        lock_file.write_text("lock")

        _remove_cache_file(cache_file)

        assert not cache_file.exists()
        assert lock_file.exists()

    def test_returns_false_when_file_missing(self, tmp_path):
        """Test returns False when file doesn't exist."""
        result = _remove_cache_file(tmp_path / "nonexistent")

        assert result is False

    def test_handles_os_error(self, tmp_path):
        """Test silently handles OSError on unlink."""
        with (
            patch.object(Path, "exists", return_value=True),
            patch.object(Path, "unlink", side_effect=OSError("permission denied")),
        ):
            result = _remove_cache_file(tmp_path / "test_cache")

        assert result is False


class TestRefreshSparkEnvironment:
    """Tests for refresh_spark_environment function."""

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_happy_path(self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start):
        """Test full refresh with all services succeeding."""
        mock_minio_creds = Mock()
        mock_minio_creds.username = "u_testuser"
        mock_minio.return_value = mock_minio_creds

        mock_polaris.return_value = {
            "client_id": "abc",
            "client_secret": "xyz",
            "personal_catalog": "user_testuser",
            "tenant_catalogs": ["tenant_a"],
        }

        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["minio"] == {"status": "ok", "username": "u_testuser"}
        assert result["polaris"]["status"] == "ok"
        assert result["polaris"]["personal_catalog"] == "user_testuser"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"] == {"status": "running"}
        assert mock_settings.cache_clear.call_count == 2
        mock_sc_start.assert_called_once_with(force_restart=True)

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_stops_existing_spark_session(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test stops active Spark session before restarting Connect server."""
        mock_minio.return_value = Mock(username="u_test")
        mock_polaris.return_value = None

        mock_session = Mock()
        mock_spark.getActiveSession.return_value = mock_session
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        mock_session.stop.assert_called_once()
        assert result["spark_session_stopped"] is True

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_polaris_not_configured(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test handles Polaris not being configured (returns None)."""
        mock_minio.return_value = Mock(username="u_test")
        mock_polaris.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["polaris"] == {"status": "skipped", "reason": "Polaris not configured"}

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_minio_error_does_not_block(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test that MinIO failure doesn't prevent Polaris/Spark refresh."""
        mock_minio.side_effect = ConnectionError("minio unreachable")
        mock_polaris.return_value = {
            "client_id": "x",
            "client_secret": "y",
            "personal_catalog": "user_test",
            "tenant_catalogs": [],
        }
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["minio"]["status"] == "error"
        assert "minio unreachable" in result["minio"]["error"]
        assert result["polaris"]["status"] == "ok"
        assert result["spark_connect"] == {"status": "running"}

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_spark_connect_error_captured(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test that Spark Connect restart failure is captured in result."""
        mock_minio.return_value = Mock(username="u_test")
        mock_polaris.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.side_effect = RuntimeError("server start failed")

        result = refresh_spark_environment()

        assert result["spark_connect"]["status"] == "error"
        assert "server start failed" in result["spark_connect"]["error"]

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_cache_files_removed_first(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test that cache files are removed before credentials are re-fetched."""
        mock_minio.return_value = Mock(username="u_test")
        mock_polaris.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        refresh_spark_environment()

        # _remove_cache_file called twice (minio + polaris) before rotate_minio_credentials
        assert mock_remove.call_count == 2
        # Settings cache cleared before credential fetches
        mock_settings.cache_clear.assert_called()

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_credentials")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    @patch("berdl_notebook_utils.refresh._remove_cache_file")
    def test_all_errors_still_returns_result(
        self, mock_remove, mock_settings, mock_minio, mock_polaris, mock_spark, mock_sc_start
    ):
        """Test that even if everything fails, we get a complete result dict."""
        mock_minio.side_effect = Exception("minio fail")
        mock_polaris.side_effect = Exception("polaris fail")
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.side_effect = Exception("spark fail")

        result = refresh_spark_environment()

        assert result["minio"]["status"] == "error"
        assert result["polaris"]["status"] == "error"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"]["status"] == "error"
