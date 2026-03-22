"""Comprehensive tests for berdl_notebook_utils.setup_trino_session."""

from unittest.mock import MagicMock, call, patch

import pytest
from governance_client.models import CredentialsResponse

from berdl_notebook_utils.berdl_settings import BERDLSettings
from berdl_notebook_utils.setup_trino_session import (
    TrinoSession,
    _build_catalog_properties,
    _catalog_exists,
    _create_dynamic_catalog,
    _sanitize_catalog_name,
    get_trino_connection,
)


# ---------------------------------------------------------------------------
# _sanitize_catalog_name
# ---------------------------------------------------------------------------
class TestSanitizeCatalogName:
    def test_simple_username(self):
        assert _sanitize_catalog_name("alice") == "alice"

    def test_uppercase_converted(self):
        assert _sanitize_catalog_name("Alice") == "alice"

    def test_mixed_case_and_numbers(self):
        assert _sanitize_catalog_name("User123") == "user123"

    def test_hyphens_replaced(self):
        assert _sanitize_catalog_name("my-user") == "my_user"

    def test_dots_replaced(self):
        assert _sanitize_catalog_name("my.user") == "my_user"

    def test_at_sign_replaced(self):
        assert _sanitize_catalog_name("user@domain") == "user_domain"

    def test_special_characters_replaced(self):
        assert _sanitize_catalog_name("u$er!name#1") == "u_er_name_1"

    def test_underscores_preserved(self):
        assert _sanitize_catalog_name("my_user_name") == "my_user_name"

    def test_already_valid(self):
        assert _sanitize_catalog_name("abc_123") == "abc_123"

    def test_empty_string(self):
        assert _sanitize_catalog_name("") == ""

    def test_all_special_chars(self):
        assert _sanitize_catalog_name("@#$%") == "____"

    def test_spaces_replaced(self):
        assert _sanitize_catalog_name("my user") == "my_user"


# ---------------------------------------------------------------------------
# _build_catalog_properties
# ---------------------------------------------------------------------------
class TestBuildCatalogProperties:
    def _make_settings(self, endpoint_url="http://minio:9000", secure=False, hive_uri="thrift://hive:9083"):
        settings = MagicMock(spec=BERDLSettings)
        settings.MINIO_ENDPOINT_URL = endpoint_url
        settings.MINIO_SECURE = secure
        settings.BERDL_HIVE_METASTORE_URI = hive_uri
        return settings

    def test_basic_properties(self):
        settings = self._make_settings()
        result = _build_catalog_properties(settings, "AKIA123", "secret456")

        assert result["hive.metastore.uri"] == "thrift://hive:9083"
        assert result["fs.native-s3.enabled"] == "true"
        assert result["s3.endpoint"] == "http://minio:9000"
        assert result["s3.aws-access-key"] == "AKIA123"
        assert result["s3.aws-secret-key"] == "secret456"
        assert result["s3.path-style-access"] == "true"
        assert result["s3.region"] == "us-east-1"

    def test_endpoint_without_protocol_insecure(self):
        settings = self._make_settings(endpoint_url="minio:9000", secure=False)
        result = _build_catalog_properties(settings, "ak", "sk")
        assert result["s3.endpoint"] == "http://minio:9000"

    def test_endpoint_without_protocol_secure(self):
        settings = self._make_settings(endpoint_url="minio:9000", secure=True)
        result = _build_catalog_properties(settings, "ak", "sk")
        assert result["s3.endpoint"] == "https://minio:9000"

    def test_endpoint_with_http_prefix_preserved(self):
        settings = self._make_settings(endpoint_url="http://minio:9000", secure=True)
        result = _build_catalog_properties(settings, "ak", "sk")
        # Should keep the existing http:// even if secure=True, because it starts with "http"
        assert result["s3.endpoint"] == "http://minio:9000"

    def test_endpoint_with_https_prefix_preserved(self):
        settings = self._make_settings(endpoint_url="https://minio:9000", secure=False)
        result = _build_catalog_properties(settings, "ak", "sk")
        assert result["s3.endpoint"] == "https://minio:9000"

    def test_returns_all_expected_keys(self):
        settings = self._make_settings()
        result = _build_catalog_properties(settings, "ak", "sk")
        expected_keys = {
            "hive.metastore.uri",
            "fs.native-s3.enabled",
            "s3.endpoint",
            "s3.aws-access-key",
            "s3.aws-secret-key",
            "s3.path-style-access",
            "s3.region",
        }
        assert set(result.keys()) == expected_keys


# ---------------------------------------------------------------------------
# _catalog_exists
# ---------------------------------------------------------------------------
class TestCatalogExists:
    def test_catalog_found(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("system",), ("u_alice_lake",), ("hive",)]
        assert _catalog_exists(cursor, "u_alice_lake") is True
        cursor.execute.assert_called_once_with("SHOW CATALOGS")

    def test_catalog_not_found(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("system",), ("hive",)]
        assert _catalog_exists(cursor, "u_alice_lake") is False

    def test_empty_catalog_list(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        assert _catalog_exists(cursor, "u_alice_lake") is False


# ---------------------------------------------------------------------------
# _create_dynamic_catalog
# ---------------------------------------------------------------------------
class TestCreateDynamicCatalog:
    def test_skips_if_catalog_exists(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = [("u_alice_lake",)]

        _create_dynamic_catalog(cursor, "u_alice_lake", "delta_lake", {"key": "val"})

        # Only SHOW CATALOGS should have been called, not CREATE CATALOG
        assert cursor.execute.call_count == 1
        assert "SHOW CATALOGS" in cursor.execute.call_args_list[0][0][0]

    def test_creates_catalog_when_not_exists(self):
        cursor = MagicMock()
        # First call is SHOW CATALOGS (returns no match), second is CREATE CATALOG
        cursor.fetchall.side_effect = [[("system",)], []]

        properties = {"hive.metastore.uri": "thrift://hive:9083", "s3.endpoint": "http://minio:9000"}
        _create_dynamic_catalog(cursor, "u_alice_lake", "delta_lake", properties)

        assert cursor.execute.call_count == 2
        create_sql = cursor.execute.call_args_list[1][0][0]
        assert "CREATE CATALOG IF NOT EXISTS" in create_sql
        assert '"u_alice_lake"' in create_sql
        assert "delta_lake" in create_sql
        assert '"hive.metastore.uri"' in create_sql
        assert "'thrift://hive:9083'" in create_sql
        assert '"s3.endpoint"' in create_sql

    def test_uses_correct_connector(self):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [[], []]

        _create_dynamic_catalog(cursor, "my_cat", "hive", {"k": "v"})

        create_sql = cursor.execute.call_args_list[1][0][0]
        assert "USING hive" in create_sql

    def test_fetchall_called_after_create(self):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [[], []]

        _create_dynamic_catalog(cursor, "cat", "delta_lake", {"a": "b"})

        # fetchall called twice: once for SHOW CATALOGS, once after CREATE CATALOG
        assert cursor.fetchall.call_count == 2


# ---------------------------------------------------------------------------
# get_trino_connection
# ---------------------------------------------------------------------------
class TestGetTrinoConnection:
    @pytest.fixture()
    def mock_settings(self):
        settings = MagicMock(spec=BERDLSettings)
        settings.USER = "testuser"
        settings.MINIO_ENDPOINT_URL = "http://minio:9000"
        settings.MINIO_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        return settings

    @pytest.fixture()
    def mock_credentials(self):
        return CredentialsResponse(
            username="testuser",
            access_key="test_access_key",
            secret_key="test_secret_key",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_returns_trino_session(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_trino.dbapi.connect.return_value = mock_conn

        result = get_trino_connection(settings=mock_settings)

        assert isinstance(result, TrinoSession)
        assert result.connection is mock_conn
        assert result.catalog == "u_testuser_lake"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_default_host_and_port(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="trino",
            port=8080,
            user="testuser",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_custom_host_and_port(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(host="custom-host", port=9999, settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="custom-host",
            port=9999,
            user="testuser",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_host_from_env_var(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials, monkeypatch
    ):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()
        monkeypatch.setenv("TRINO_HOST", "env-trino-host")
        monkeypatch.setenv("TRINO_PORT", "7777")

        get_trino_connection(settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="env-trino-host",
            port=7777,
            user="testuser",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_explicit_host_overrides_env(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials, monkeypatch
    ):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()
        monkeypatch.setenv("TRINO_HOST", "env-host")
        monkeypatch.setenv("TRINO_PORT", "1111")

        get_trino_connection(host="explicit-host", port=2222, settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="explicit-host",
            port=2222,
            user="testuser",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_custom_catalog_suffix(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        result = get_trino_connection(catalog_suffix="research_team", settings=mock_settings)

        assert result.catalog == "u_testuser_research_team"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_custom_connector(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_trino.dbapi.connect.return_value = mock_conn

        get_trino_connection(connector="hive", settings=mock_settings)

        mock_create_cat.assert_called_once()
        args = mock_create_cat.call_args
        assert args[0][2] == "hive"  # connector argument

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_credentials_passed_to_catalog_properties(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(settings=mock_settings)

        mock_create_cat.assert_called_once()
        properties = mock_create_cat.call_args[0][3]
        assert properties["s3.aws-access-key"] == "test_access_key"
        assert properties["s3.aws-secret-key"] == "test_secret_key"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_username_sanitized_in_catalog_name(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_settings.USER = "My-User.Name"
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        result = get_trino_connection(settings=mock_settings)

        assert result.catalog == "u_my_user_name_lake"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    @patch("berdl_notebook_utils.setup_trino_session.get_settings")
    def test_loads_settings_when_none(self, mock_get_settings, mock_get_creds, mock_trino, mock_create_cat):
        settings = MagicMock(spec=BERDLSettings)
        settings.USER = "autouser"
        settings.MINIO_ENDPOINT_URL = "http://minio:9000"
        settings.MINIO_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        mock_get_settings.return_value = settings
        mock_get_creds.return_value = CredentialsResponse(
            username="autouser", access_key="ak", secret_key="sk"
        )
        mock_trino.dbapi.connect.return_value = MagicMock()

        result = get_trino_connection()

        mock_get_settings.cache_clear.assert_called_once()
        mock_get_settings.assert_called_once()
        assert result.catalog == "u_autouser_lake"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_minio_credentials")
    def test_cursor_created_from_connection(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        get_trino_connection(settings=mock_settings)

        mock_conn.cursor.assert_called_once()
        mock_create_cat.assert_called_once()
        assert mock_create_cat.call_args[0][0] is mock_cursor


# ---------------------------------------------------------------------------
# TrinoSession named tuple
# ---------------------------------------------------------------------------
class TestTrinoSession:
    def test_is_named_tuple(self):
        conn = MagicMock()
        session = TrinoSession(connection=conn, catalog="my_cat")
        assert session.connection is conn
        assert session.catalog == "my_cat"

    def test_unpacking(self):
        conn = MagicMock()
        session = TrinoSession(connection=conn, catalog="my_cat")
        unpacked_conn, unpacked_cat = session
        assert unpacked_conn is conn
        assert unpacked_cat == "my_cat"

    def test_indexing(self):
        conn = MagicMock()
        session = TrinoSession(connection=conn, catalog="my_cat")
        assert session[0] is conn
        assert session[1] == "my_cat"
