"""Tests for berdl_notebook_utils.refresh module."""

from unittest.mock import Mock, patch

from berdl_notebook_utils.refresh import refresh_spark_environment


class TestRefreshSparkEnvironment:
    """Tests for refresh_spark_environment function."""

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_full_refresh_success(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test successful full refresh with all steps."""
        mock_minio_creds = Mock()
        mock_minio_creds.username = "testuser"
        mock_rotate_minio.return_value = mock_minio_creds
        mock_spark_session.getActiveSession.return_value = None
        mock_start_connect.return_value = {"pid": 123, "port": 15002, "url": "sc://localhost:15002"}

        result = refresh_spark_environment()

        assert result["minio"] == {"status": "ok", "username": "testuser"}
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"] == {"pid": 123, "port": 15002, "url": "sc://localhost:15002"}
        assert mock_get_settings.cache_clear.call_count == 2
        mock_rotate_minio.assert_called_once()
        mock_start_connect.assert_called_once_with(force_restart=True)

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_stops_existing_spark_session(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test that an existing Spark session is stopped."""
        mock_rotate_minio.return_value = Mock(username="testuser")
        mock_existing_session = Mock()
        mock_spark_session.getActiveSession.return_value = mock_existing_session
        mock_start_connect.return_value = {"pid": 456}

        result = refresh_spark_environment()

        assert result["spark_session_stopped"] is True
        mock_existing_session.stop.assert_called_once()

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_minio_rotation_failure_continues(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test that MinIO rotation failure doesn't stop the rest of the refresh."""
        mock_rotate_minio.side_effect = RuntimeError("API unavailable")
        mock_spark_session.getActiveSession.return_value = None
        mock_start_connect.return_value = {"pid": 789}

        result = refresh_spark_environment()

        assert result["minio"]["status"] == "error"
        assert "API unavailable" in result["minio"]["error"]
        assert result["spark_connect"] == {"pid": 789}
        mock_start_connect.assert_called_once_with(force_restart=True)

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_spark_connect_failure_captured(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test that Spark Connect failure is captured in result."""
        mock_rotate_minio.return_value = Mock(username="testuser")
        mock_spark_session.getActiveSession.return_value = None
        mock_start_connect.side_effect = RuntimeError("start script not found")

        result = refresh_spark_environment()

        assert result["minio"]["status"] == "ok"
        assert result["spark_connect"]["status"] == "error"
        assert "start script not found" in result["spark_connect"]["error"]

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_both_failures_captured(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test that failures in both MinIO and Spark Connect are captured."""
        mock_rotate_minio.side_effect = ConnectionError("MMS down")
        mock_spark_session.getActiveSession.return_value = None
        mock_start_connect.side_effect = FileNotFoundError("no start script")

        result = refresh_spark_environment()

        assert result["minio"]["status"] == "error"
        assert result["spark_connect"]["status"] == "error"
        assert result["spark_session_stopped"] is False

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.rotate_minio_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_settings_cache_cleared_twice(
        self,
        mock_get_settings,
        mock_rotate_minio,
        mock_spark_session,
        mock_start_connect,
    ):
        """Test that get_settings cache is cleared before and after credential rotation."""
        mock_rotate_minio.return_value = Mock(username="testuser")
        mock_spark_session.getActiveSession.return_value = None
        mock_start_connect.return_value = {}

        refresh_spark_environment()

        # Should be cleared once before rotation and once after
        assert mock_get_settings.cache_clear.call_count == 2
