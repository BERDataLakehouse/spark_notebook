"""Tests for refresh.py - Credential and Spark + Trino environment refresh."""

from unittest.mock import MagicMock, Mock, patch

from berdl_notebook_utils.refresh import refresh_spark_environment


def _polaris_unconfigured_settings() -> MagicMock:
    """Return a get_settings()-like mock whose POLARIS_CATALOG_URI is falsy.

    Tests that don't care about Step 7 (Trino refresh) use this to make
    refresh_spark_environment() short-circuit Step 7 without needing to
    mock the trino client.
    """
    settings = MagicMock()
    settings.POLARIS_CATALOG_URI = None
    return settings


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
        # Step 7 short-circuit: report Polaris unconfigured for this test.
        mock_settings.return_value = _polaris_unconfigured_settings()

        result = refresh_spark_environment()

        assert result["credentials"] == {"status": "ok", "username": "u_testuser"}
        assert result["polaris_catalog"]["status"] == "ok"
        assert result["polaris_catalog"]["personal_catalog"] == "user_testuser"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"] == {"status": "running"}
        assert mock_settings.cache_clear.call_count == 2
        mock_sc_start.assert_called_once_with(force_restart=True)
        assert result["trino_catalogs"] == {"status": "skipped", "reason": "Polaris not configured"}

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
        mock_settings.return_value = _polaris_unconfigured_settings()

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
        mock_settings.return_value = _polaris_unconfigured_settings()

        result = refresh_spark_environment()

        assert result["polaris_catalog"] == {"status": "skipped", "reason": "Polaris not configured"}
        assert result["trino_catalogs"] == {"status": "skipped", "reason": "Polaris not configured"}

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
        mock_settings.return_value = _polaris_unconfigured_settings()

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
        mock_settings.return_value = _polaris_unconfigured_settings()

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
        mock_settings.return_value = _polaris_unconfigured_settings()

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
        mock_settings.return_value = _polaris_unconfigured_settings()

        result = refresh_spark_environment()

        assert result["credentials"]["status"] == "error"
        assert result["polaris_catalog"]["status"] == "error"
        assert result["spark_session_stopped"] is False
        assert result["spark_connect"]["status"] == "error"
        # Step 7 short-circuits when POLARIS_CATALOG_URI is unset.
        assert result["trino_catalogs"] == {"status": "skipped", "reason": "Polaris not configured"}


class TestRefreshSparkEnvironmentTrinoStep:
    """Tests for Step 7 — Trino dynamic catalog refresh after credential rotation."""

    def _setup_common_mocks(self, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start):
        mock_creds.return_value = Mock(username="u_test")
        mock_polaris_catalog.return_value = {
            "personal_catalog": "user_test",
            "tenant_catalogs": ["tenant_a"],
        }
        mock_spark.getActiveSession.return_value = None
        mock_sc_start.return_value = {"status": "running"}

    @patch("berdl_notebook_utils.setup_trino_session.get_trino_connection")
    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_trino_refresh_invoked_when_polaris_configured(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start, mock_get_trino,
    ):
        """When POLARIS_CATALOG_URI is set, Step 7 must call get_trino_connection().

        This is what re-creates the per-user Polaris dynamic catalogs in the
        Trino coordinator with the freshly-rotated POLARIS_CREDENTIAL — the
        whole reason this step exists.
        """
        self._setup_common_mocks(mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start)
        polaris_settings = MagicMock()
        polaris_settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value = polaris_settings

        mock_conn = Mock()
        mock_get_trino.return_value = mock_conn

        result = refresh_spark_environment()

        mock_get_trino.assert_called_once_with()
        mock_conn.close.assert_called_once()
        assert result["trino_catalogs"] == {"status": "ok"}

    @patch("berdl_notebook_utils.setup_trino_session.get_trino_connection")
    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_trino_refresh_error_does_not_crash_refresh(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start, mock_get_trino,
    ):
        """A Trino-side failure must be captured but must not raise out of refresh()."""
        self._setup_common_mocks(mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start)
        polaris_settings = MagicMock()
        polaris_settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value = polaris_settings

        mock_get_trino.side_effect = ConnectionError("trino unreachable")

        result = refresh_spark_environment()

        assert result["trino_catalogs"]["status"] == "error"
        assert "trino unreachable" in result["trino_catalogs"]["error"]
        # Other steps still succeeded — refresh() must not propagate.
        assert result["spark_connect"] == {"status": "running"}

    @patch("berdl_notebook_utils.setup_trino_session.get_trino_connection")
    @patch("berdl_notebook_utils.refresh.start_spark_connect_server")
    @patch("berdl_notebook_utils.refresh.SparkSession")
    @patch("berdl_notebook_utils.refresh.get_polaris_catalog_info")
    @patch("berdl_notebook_utils.refresh.rotate_credentials")
    @patch("berdl_notebook_utils.refresh.get_settings")
    def test_trino_refresh_skipped_when_polaris_not_configured(
        self, mock_settings, mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start, mock_get_trino,
    ):
        """If POLARIS_CATALOG_URI is unset, Step 7 must NOT call get_trino_connection()."""
        self._setup_common_mocks(mock_creds, mock_polaris_catalog, mock_spark, mock_sc_start)
        mock_settings.return_value = _polaris_unconfigured_settings()

        result = refresh_spark_environment()

        mock_get_trino.assert_not_called()
        assert result["trino_catalogs"] == {"status": "skipped", "reason": "Polaris not configured"}
