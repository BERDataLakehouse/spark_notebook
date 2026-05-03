"""Tests for refresh.py - Credential and Spark environment refresh."""

from unittest.mock import Mock, patch

from berdl_notebook_utils.refresh import refresh_spark_environment


class TestRefreshSparkEnvironment:
    """Tests for refresh_spark_environment function."""

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_happy_path(self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start):
        """Test full refresh with all services succeeding."""
        mock_unified_creds = Mock()
        mock_unified_creds.username = "u_testuser"
        mock_creds.return_value = mock_unified_creds

        mock_polaris_catalog.return_value = {
            "personal_catalog": "user_testuser",
            "tenant_catalogs": ["tenant_a"],
        }

        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["credentials"] == {"status": "ok", "username": "u_testuser"}
        assert result["polaris_catalog"]["status"] == "ok"
        assert result["polaris_catalog"]["personal_catalog"] == "user_testuser"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"] == {"status": "running"}
        assert mock_settings.cache_clear.call_count == 2
        mock_sc_start.assert_called_once_with(force_restart=True)

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_stops_existing_spark_session(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start
    ):
        """Test stops active Spark session before restarting Connect server."""
        mock_creds.return_value = Mock(username="u_test")
        mock_polaris_catalog.return_value = None

        mock_session = Mock()
        mock_spark.getActiveSession.return_value = mock_session
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        mock_session.stop.assert_called_once()
        assert result["spark_session_stopped"] is True

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_polaris_not_configured(self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start):
        """Test handles Polaris not being configured (returns None)."""
        mock_creds.return_value = Mock(username="u_test")
        mock_polaris_catalog.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["polaris_catalog"] == {"status": "skipped", "reason": "Polaris not configured"}

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_credential_error_does_not_block(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start
    ):
        """Test that a credential rotation failure doesn't prevent catalog/Spark refresh."""
        mock_creds.side_effect = ConnectionError("mms unreachable")
        mock_polaris_catalog.return_value = {
            "personal_catalog": "user_test",
            "tenant_catalogs": [],
        }
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        result = refresh_spark_environment()

        assert result["credentials"]["status"] == "error"
        assert "mms unreachable" in result["credentials"]["error"]
        assert result["polaris_catalog"]["status"] == "ok"
        assert result["spark_connect"] == {"status": "running"}

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_spark_connect_error_captured(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start
    ):
        """Test that Spark Connect restart failure is captured in result."""
        mock_creds.return_value = Mock(username="u_test")
        mock_polaris_catalog.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.side_effect = RuntimeError("server start failed")

        result = refresh_spark_environment()

        assert result["spark_connect"]["status"] == "error"
        assert "server start failed" in result["spark_connect"]["error"]

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_settings_cleared_before_credentials_are_refetched(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start
    ):
        """Test settings cache is cleared before credentials are re-fetched."""
        mock_creds.return_value = Mock(username="u_test")
        mock_polaris_catalog.return_value = None
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

        refresh_spark_environment()

        mock_settings.cache_clear.assert_called()

    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_all_errors_still_returns_result(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start
    ):
        """Test that even if everything fails, we get a complete result dict."""
        mock_creds.side_effect = Exception("minio fail")
        mock_polaris_catalog.side_effect = Exception("polaris fail")
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.side_effect = Exception("spark fail")

        result = refresh_spark_environment()

        assert result["credentials"]["status"] == "error"
        assert result["polaris_catalog"]["status"] == "error"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"]["status"] == "error"
