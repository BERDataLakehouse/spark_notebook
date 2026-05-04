"""Comprehensive tests for berdl_notebook_utils.setup_trino_session."""

from unittest.mock import MagicMock, patch

import pytest
from governance_client.models import CredentialsResponse

from berdl_notebook_utils.berdl_settings import BERDLSettings
from berdl_notebook_utils.setup_trino_session import (
    ALLOWED_CONNECTORS,
    _build_catalog_properties,
    _build_iceberg_catalog_properties,
    _catalog_exists,
    _create_dynamic_catalog,
    _create_polaris_catalogs,
    _escape_sql_string,
    _get_personal_catalog_alias,
    _get_polaris_oauth2_server_uri,
    _get_tenant_catalog_alias,
    _iter_tenant_catalogs,
    _sanitize_identifier,
    _validate_connector,
    get_trino_connection,
)


# ---------------------------------------------------------------------------
# _sanitize_identifier
# ---------------------------------------------------------------------------
class TestSanitizeCatalogName:
    def test_simple_username(self):
        assert _sanitize_identifier("alice") == "alice"

    def test_uppercase_converted(self):
        assert _sanitize_identifier("Alice") == "alice"

    def test_mixed_case_and_numbers(self):
        assert _sanitize_identifier("User123") == "user123"

    def test_hyphens_replaced(self):
        assert _sanitize_identifier("my-user") == "my_user"

    def test_dots_replaced(self):
        assert _sanitize_identifier("my.user") == "my_user"

    def test_at_sign_replaced(self):
        assert _sanitize_identifier("user@domain") == "user_domain"

    def test_special_characters_replaced(self):
        assert _sanitize_identifier("u$er!name#1") == "u_er_name_1"

    def test_underscores_preserved(self):
        assert _sanitize_identifier("my_user_name") == "my_user_name"

    def test_already_valid(self):
        assert _sanitize_identifier("abc_123") == "abc_123"

    def test_empty_string(self):
        assert _sanitize_identifier("") == ""

    def test_all_special_chars(self):
        assert _sanitize_identifier("@#$%") == "____"

    def test_spaces_replaced(self):
        assert _sanitize_identifier("my user") == "my_user"


# ---------------------------------------------------------------------------
# _build_catalog_properties
# ---------------------------------------------------------------------------
class TestBuildCatalogProperties:
    def _make_settings(self, endpoint_url="http://minio:9000", secure=False, hive_uri="thrift://hive:9083"):
        settings = MagicMock(spec=BERDLSettings)
        settings.S3_ENDPOINT_URL = endpoint_url
        settings.S3_SECURE = secure
        settings.BERDL_HIVE_METASTORE_URI = hive_uri
        settings.POLARIS_CATALOG_URI = None
        settings.POLARIS_CREDENTIAL = None
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = None
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
# Polaris/Iceberg catalog helpers
# ---------------------------------------------------------------------------
class TestPolarisCatalogHelpers:
    def _make_settings(self, polaris_uri="http://polaris:8181/api/catalog"):
        settings = MagicMock(spec=BERDLSettings)
        settings.S3_ENDPOINT_URL = "minio:9000"
        settings.S3_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        settings.POLARIS_CATALOG_URI = polaris_uri
        settings.POLARIS_CREDENTIAL = "client:secret"
        settings.POLARIS_PERSONAL_CATALOG = "user_testuser"
        settings.POLARIS_TENANT_CATALOGS = "tenant_globalusers,tenant_kbase"
        return settings

    def test_personal_catalog_alias_strips_user_prefix(self):
        assert _get_personal_catalog_alias("user_tgu2") == "tgu2"

    def test_personal_catalog_alias_sanitizes(self):
        assert _get_personal_catalog_alias("user_Alice-Lake") == "alice_lake"

    def test_personal_catalog_alias_none_when_missing(self):
        assert _get_personal_catalog_alias(None) is None

    def test_tenant_catalog_alias_strips_tenant_prefix(self):
        assert _get_tenant_catalog_alias("tenant_globalusers") == "globalusers"

    def test_tenant_catalog_alias_sanitizes(self):
        assert _get_tenant_catalog_alias("tenant_KBase-Dev") == "kbase_dev"

    def test_iter_tenant_catalogs_ignores_blanks(self):
        assert _iter_tenant_catalogs("tenant_a, ,tenant_b,") == ["tenant_a", "tenant_b"]

    def test_oauth2_server_uri_appends_v1(self):
        settings = self._make_settings("http://polaris:8181/api/catalog")
        assert _get_polaris_oauth2_server_uri(settings) == "http://polaris:8181/api/catalog/v1/oauth/tokens"

    def test_oauth2_server_uri_handles_v1_uri(self):
        settings = self._make_settings("http://polaris:8181/api/catalog/v1")
        assert _get_polaris_oauth2_server_uri(settings) == "http://polaris:8181/api/catalog/v1/oauth/tokens"

    def test_build_iceberg_catalog_properties(self):
        settings = self._make_settings()

        result = _build_iceberg_catalog_properties(settings, "user_testuser", "ak", "sk")

        assert result["iceberg.catalog.type"] == "rest"
        assert result["iceberg.rest-catalog.uri"] == "http://polaris:8181/api/catalog"
        assert result["iceberg.rest-catalog.warehouse"] == "user_testuser"
        assert result["iceberg.rest-catalog.security"] == "OAUTH2"
        assert result["iceberg.rest-catalog.oauth2.credential"] == "client:secret"
        assert result["iceberg.rest-catalog.oauth2.scope"] == "PRINCIPAL_ROLE:ALL"
        assert result["iceberg.rest-catalog.oauth2.server-uri"] == "http://polaris:8181/api/catalog/v1/oauth/tokens"
        assert result["iceberg.rest-catalog.vended-credentials-enabled"] == "false"
        assert result["iceberg.security"] == "read_only"
        assert result["fs.native-s3.enabled"] == "true"
        assert result["s3.endpoint"] == "http://minio:9000"
        assert result["s3.aws-access-key"] == "ak"
        assert result["s3.aws-secret-key"] == "sk"
        assert "hive.metastore.uri" not in result

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    def test_create_polaris_catalogs_creates_personal_and_tenant_aliases(self, mock_create_cat):
        settings = self._make_settings()
        cursor = MagicMock()

        _create_polaris_catalogs(cursor, settings, "ak", "sk")

        calls = mock_create_cat.call_args_list
        assert [call[0][1] for call in calls] == ["testuser", "globalusers", "kbase"]
        assert [call[0][2] for call in calls] == ["iceberg", "iceberg", "iceberg"]
        assert calls[0][0][3]["iceberg.rest-catalog.warehouse"] == "user_testuser"
        assert calls[1][0][3]["iceberg.rest-catalog.warehouse"] == "tenant_globalusers"
        assert calls[2][0][3]["iceberg.rest-catalog.warehouse"] == "tenant_kbase"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    def test_create_polaris_catalogs_skips_when_not_configured(self, mock_create_cat):
        settings = self._make_settings()
        settings.POLARIS_CATALOG_URI = None
        cursor = MagicMock()

        _create_polaris_catalogs(cursor, settings, "ak", "sk")

        mock_create_cat.assert_not_called()


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

    def test_force_drops_then_recreates_when_catalog_exists(self):
        """force=True must DROP the existing catalog before recreating it.

        This is the recovery path used by the Polaris/Iceberg flow so that
        a rotated POLARIS_CREDENTIAL takes effect — without the DROP, the
        stale oauth2.credential cached in the coordinator-loaded catalog
        keeps producing ICEBERG_CATALOG_ERROR even with
        token-refresh-enabled=true.
        """
        cursor = MagicMock()
        # SHOW CATALOGS sees the catalog (so we go through the drop path);
        # DROP CATALOG and CREATE CATALOG both return empty.
        cursor.fetchall.side_effect = [[("my",)], [], []]

        _create_dynamic_catalog(
            cursor,
            "my",
            "iceberg",
            {"iceberg.rest-catalog.oauth2.credential": "id:secret"},
            force=True,
        )

        sqls = [call.args[0] for call in cursor.execute.call_args_list]
        # Three statements expected, in this order:
        #   1. SHOW CATALOGS    (existence check)
        #   2. DROP CATALOG     (force-recreate)
        #   3. CREATE CATALOG   (with the new properties)
        assert len(sqls) == 3
        assert "SHOW CATALOGS" in sqls[0]
        assert 'DROP CATALOG IF EXISTS "my"' in sqls[1]
        assert "CREATE CATALOG IF NOT EXISTS" in sqls[2]
        assert '"my"' in sqls[2]
        assert "USING iceberg" in sqls[2]
        # fetchall called once per execute()
        assert cursor.fetchall.call_count == 3

    def test_force_just_creates_when_catalog_absent(self):
        """force=True is a no-op (no DROP) when the catalog doesn't exist yet."""
        cursor = MagicMock()
        cursor.fetchall.side_effect = [[("system",)], []]

        _create_dynamic_catalog(cursor, "tgu2", "iceberg", {"k": "v"}, force=True)

        sqls = [call.args[0] for call in cursor.execute.call_args_list]
        # Two statements: SHOW CATALOGS, then CREATE CATALOG.  No DROP because
        # the catalog wasn't there.
        assert len(sqls) == 2
        assert "SHOW CATALOGS" in sqls[0]
        assert "DROP CATALOG" not in sqls[0] and "DROP CATALOG" not in sqls[1]
        assert "CREATE CATALOG IF NOT EXISTS" in sqls[1]

    def test_default_force_false_still_skips_existing(self):
        """Sanity: omitting force keeps the original skip-if-exists behaviour."""
        cursor = MagicMock()
        cursor.fetchall.return_value = [("u_alice_lake",)]

        _create_dynamic_catalog(cursor, "u_alice_lake", "delta_lake", {"key": "val"})

        # Only SHOW CATALOGS — no DROP, no CREATE.
        assert cursor.execute.call_count == 1
        assert "SHOW CATALOGS" in cursor.execute.call_args[0][0]


# ---------------------------------------------------------------------------
# get_trino_connection
# ---------------------------------------------------------------------------
class TestGetTrinoConnection:
    @pytest.fixture()
    def mock_settings(self):
        settings = MagicMock(spec=BERDLSettings)
        settings.USER = "testuser"
        settings.S3_ENDPOINT_URL = "http://minio:9000"
        settings.S3_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        settings.TRINO_HOST = "trino"
        settings.TRINO_PORT = 8080
        settings.KBASE_AUTH_TOKEN = "fake-kbase-token"
        settings.POLARIS_CATALOG_URI = None
        settings.POLARIS_CREDENTIAL = None
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = None
        return settings

    @pytest.fixture()
    def mock_credentials(self):
        return CredentialsResponse(
            username="testuser",
            s3_access_key="test_access_key",
            s3_secret_key="test_secret_key",
            polaris_client_id="test_polaris_id",
            polaris_client_secret="test_polaris_secret",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_returns_connection(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_trino.dbapi.connect.return_value = mock_conn

        result = get_trino_connection(settings=mock_settings)

        assert result is mock_conn

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_sets_default_catalog_on_connection(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        """Connection's default catalog is set so queries use schema.table without prefix."""
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_trino.dbapi.connect.return_value = mock_conn

        get_trino_connection(settings=mock_settings)

        assert mock_conn._client_session.catalog == "u_testuser"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_default_host_and_port(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="trino",
            port=8080,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_custom_host_and_port(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(host="custom-host", port=9999, settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="custom-host",
            port=9999,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_host_and_port_from_settings(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_settings.TRINO_HOST = "settings-trino-host"
        mock_settings.TRINO_PORT = 7777
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="settings-trino-host",
            port=7777,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_explicit_host_overrides_settings(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_settings.TRINO_HOST = "settings-host"
        mock_settings.TRINO_PORT = 1111
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(host="explicit-host", port=2222, settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="explicit-host",
            port=2222,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
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
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
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
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_username_sanitized_in_catalog_name(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_settings.USER = "My-User.Name"
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        result = get_trino_connection(settings=mock_settings)

        assert result._client_session.catalog == "u_my_user_name"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    @patch("berdl_notebook_utils.setup_trino_session.get_settings")
    def test_loads_settings_when_none(self, mock_get_settings, mock_get_creds, mock_trino, mock_create_cat):
        settings = MagicMock(spec=BERDLSettings)
        settings.USER = "autouser"
        settings.S3_ENDPOINT_URL = "http://minio:9000"
        settings.S3_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        settings.TRINO_HOST = "trino"
        settings.TRINO_PORT = 8080
        settings.KBASE_AUTH_TOKEN = "fake-kbase-token"
        settings.POLARIS_CATALOG_URI = None
        settings.POLARIS_CREDENTIAL = None
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = None
        mock_get_settings.return_value = settings
        mock_get_creds.return_value = CredentialsResponse(
            username="autouser",
            s3_access_key="ak",
            s3_secret_key="sk",
            polaris_client_id="pid",
            polaris_client_secret="psecret",
        )
        mock_trino.dbapi.connect.return_value = MagicMock()

        result = get_trino_connection()

        mock_get_settings.cache_clear.assert_called_once()
        mock_get_settings.assert_called_once()
        assert result._client_session.catalog == "u_autouser"

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
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

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_creates_polaris_catalogs_when_configured(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        mock_settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.POLARIS_CREDENTIAL = "client:secret"
        mock_settings.POLARIS_PERSONAL_CATALOG = "user_testuser"
        mock_settings.POLARIS_TENANT_CATALOGS = "tenant_globalusers,tenant_kbase"
        mock_get_creds.return_value = mock_credentials
        mock_conn = MagicMock()
        mock_trino.dbapi.connect.return_value = mock_conn

        get_trino_connection(settings=mock_settings)

        calls = mock_create_cat.call_args_list
        assert [call[0][1] for call in calls] == ["u_testuser", "testuser", "globalusers", "kbase"]
        assert [call[0][2] for call in calls] == ["delta_lake", "iceberg", "iceberg", "iceberg"]
        assert calls[1][0][3]["iceberg.rest-catalog.warehouse"] == "user_testuser"
        assert calls[2][0][3]["iceberg.rest-catalog.warehouse"] == "tenant_globalusers"
        assert calls[3][0][3]["iceberg.rest-catalog.warehouse"] == "tenant_kbase"
        assert mock_conn._client_session.catalog == "u_testuser"


# ---------------------------------------------------------------------------
# _escape_sql_string
# ---------------------------------------------------------------------------
class TestEscapeSqlString:
    def test_no_quotes(self):
        assert _escape_sql_string("hello world") == "hello world"

    def test_single_quote(self):
        assert _escape_sql_string("it's") == "it''s"

    def test_multiple_quotes(self):
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_empty_string(self):
        assert _escape_sql_string("") == ""

    def test_consecutive_quotes(self):
        assert _escape_sql_string("a''b") == "a''''b"


# ---------------------------------------------------------------------------
# _validate_connector
# ---------------------------------------------------------------------------
class TestValidateConnector:
    def test_allowed_connectors_pass(self):
        for connector in ALLOWED_CONNECTORS:
            _validate_connector(connector)  # Should not raise

    def test_invalid_connector_raises(self):
        with pytest.raises(ValueError, match="not allowed"):
            _validate_connector("malicious; DROP TABLE")

    def test_unknown_connector_raises(self):
        with pytest.raises(ValueError, match="not allowed"):
            _validate_connector("postgresql")

    def test_error_message_lists_allowed(self):
        with pytest.raises(ValueError, match="delta_lake"):
            _validate_connector("bad")


# ---------------------------------------------------------------------------
# _create_dynamic_catalog — SQL injection / escaping tests
# ---------------------------------------------------------------------------
class TestCreateDynamicCatalogSecurity:
    def test_rejects_invalid_connector(self):
        cursor = MagicMock()
        cursor.fetchall.return_value = []  # catalog not found

        with pytest.raises(ValueError, match="not allowed"):
            _create_dynamic_catalog(cursor, "my_cat", "evil_connector", {"k": "v"})

    def test_escapes_single_quotes_in_property_values(self):
        cursor = MagicMock()
        cursor.fetchall.side_effect = [[], []]  # not found, then create

        _create_dynamic_catalog(cursor, "my_cat", "delta_lake", {"s3.aws-secret-key": "secret'with'quotes"})

        create_sql = cursor.execute.call_args_list[1][0][0]
        assert "secret''with''quotes" in create_sql
        assert "secret'with'quotes" not in create_sql


# ---------------------------------------------------------------------------
# get_trino_connection — falsy host/port values honored
# ---------------------------------------------------------------------------
class TestGetTrinoConnectionFalsyValues:
    @pytest.fixture()
    def mock_settings(self):
        settings = MagicMock(spec=BERDLSettings)
        settings.USER = "testuser"
        settings.S3_ENDPOINT_URL = "http://minio:9000"
        settings.S3_SECURE = False
        settings.BERDL_HIVE_METASTORE_URI = "thrift://hive:9083"
        settings.TRINO_HOST = "trino"
        settings.TRINO_PORT = 8080
        settings.KBASE_AUTH_TOKEN = "fake-kbase-token"
        settings.POLARIS_CATALOG_URI = None
        settings.POLARIS_CREDENTIAL = None
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = None
        return settings

    @pytest.fixture()
    def mock_credentials(self):
        return CredentialsResponse(
            username="testuser",
            s3_access_key="test_access_key",
            s3_secret_key="test_secret_key",
            polaris_client_id="test_polaris_id",
            polaris_client_secret="test_polaris_secret",
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_empty_string_host_is_honored(
        self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials
    ):
        """Empty-string host should NOT fall through to settings default."""
        mock_settings.TRINO_HOST = "settings-host"
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(host="", settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="",
            port=8080,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )

    @patch("berdl_notebook_utils.setup_trino_session._create_dynamic_catalog")
    @patch("berdl_notebook_utils.setup_trino_session.trino")
    @patch("berdl_notebook_utils.setup_trino_session.get_credentials")
    def test_port_zero_is_honored(self, mock_get_creds, mock_trino, mock_create_cat, mock_settings, mock_credentials):
        """port=0 should NOT fall through to settings default."""
        mock_settings.TRINO_PORT = 9999
        mock_get_creds.return_value = mock_credentials
        mock_trino.dbapi.connect.return_value = MagicMock()

        get_trino_connection(port=0, settings=mock_settings)

        mock_trino.dbapi.connect.assert_called_once_with(
            host="trino",
            port=0,
            user="testuser",
            extra_credential=[("kbase_auth_token", "fake-kbase-token")],
        )
