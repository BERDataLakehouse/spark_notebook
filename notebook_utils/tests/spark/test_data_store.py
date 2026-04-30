"""Tests for spark/data_store.py — Iceberg + Hive listing with prefix filtering."""

import json
from unittest.mock import MagicMock, Mock, patch

import pytest

from berdl_notebook_utils.spark import data_store
from berdl_notebook_utils.spark.data_store import (
    _aliases_for_user,
    _filter_to_user_namespaces,
    _format_output,
    _list_iceberg_catalogs,
    _list_iceberg_namespaces,
    _list_hive_databases,
    get_databases,
    get_db_structure,
    get_table_schema,
    get_tables,
    invalidate_cache,
)


@pytest.fixture(autouse=True)
def _clear_data_store_caches():
    """Reset in-process TTL caches before/after each test for isolation."""
    invalidate_cache()
    yield
    invalidate_cache()


# =============================================================================
# Helpers
# =============================================================================


def _make_set_rows(*catalog_names: str) -> list:
    """Create mock SET command rows for catalog configs.

    For each catalog name, generates the top-level ``spark.sql.catalog.<name>``
    key plus a few sub-property keys to simulate realistic SET output.
    """
    rows = []
    for name in catalog_names:
        rows.append({"key": f"spark.sql.catalog.{name}", "value": "org.apache.iceberg.spark.SparkCatalog"})
        rows.append({"key": f"spark.sql.catalog.{name}.type", "value": "rest"})
        rows.append({"key": f"spark.sql.catalog.{name}.uri", "value": "http://polaris:8181/api/catalog"})
    rows.append({"key": "spark.app.name", "value": "test"})
    rows.append({"key": "spark.sql.extensions", "value": "io.delta.sql.DeltaSparkSessionExtension"})
    return rows


# =============================================================================
# _format_output
# =============================================================================


class TestFormatOutput:
    """Tests for _format_output helper."""

    def test_format_output_json(self):
        result = _format_output(["item1", "item2"], return_json=True)
        assert json.loads(result) == ["item1", "item2"]

    def test_format_output_raw(self):
        data = ["item1", "item2"]
        result = _format_output(data, return_json=False)
        assert result == data

    def test_format_complex_data_as_json(self):
        data = {"my.demo": ["t1", "t2"], "u_alice__demo": ["t3"]}
        result = _format_output(data, return_json=True)
        assert json.loads(result) == data


# =============================================================================
# _list_iceberg_catalogs
# =============================================================================


class TestListIcebergCatalogs:
    """Tests for _list_iceberg_catalogs."""

    def test_excludes_spark_catalog(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = _make_set_rows("spark_catalog", "my", "kbase")

        result = _list_iceberg_catalogs(spark)

        assert "spark_catalog" not in result
        assert result == ["kbase", "my"]

    def test_returns_sorted(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = _make_set_rows("zebra", "alpha")

        result = _list_iceberg_catalogs(spark)

        assert result == ["alpha", "zebra"]

    def test_empty_when_only_spark_catalog(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = _make_set_rows("spark_catalog")

        result = _list_iceberg_catalogs(spark)

        assert result == []

    def test_ignores_subproperty_keys(self):
        """Sub-property keys like spark.sql.catalog.my.type must not be matched as catalogs."""
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [
            {"key": "spark.sql.catalog.my", "value": "..."},
            {"key": "spark.sql.catalog.my.type", "value": "rest"},
            {"key": "spark.sql.catalog.my.uri", "value": "http://x"},
        ]

        result = _list_iceberg_catalogs(spark)

        assert result == ["my"]


# =============================================================================
# _list_iceberg_namespaces
# =============================================================================


class TestListIcebergNamespaces:
    """Tests for _list_iceberg_namespaces."""

    def test_returns_catalog_namespace_format(self):
        spark = MagicMock()

        with patch.object(data_store, "_list_iceberg_catalogs", return_value=["kbase", "my"]):

            def sql_side_effect(query):
                result = MagicMock()
                if "SHOW NAMESPACES IN kbase" in query:
                    result.collect.return_value = [{"namespace": "shared"}, {"namespace": "research"}]
                elif "SHOW NAMESPACES IN my" in query:
                    result.collect.return_value = [{"namespace": "demo"}]
                return result

            spark.sql.side_effect = sql_side_effect

            result = _list_iceberg_namespaces(spark)

        assert sorted(result) == ["kbase.research", "kbase.shared", "my.demo"]

    def test_skips_inaccessible_catalog(self):
        spark = MagicMock()

        with patch.object(data_store, "_list_iceberg_catalogs", return_value=["my", "broken"]):

            def sql_side_effect(query):
                if "broken" in query:
                    raise Exception("Catalog not accessible")
                result = MagicMock()
                result.collect.return_value = [{"namespace": "demo"}]
                return result

            spark.sql.side_effect = sql_side_effect

            result = _list_iceberg_namespaces(spark)

        assert result == ["my.demo"]

    def test_empty_when_no_catalogs(self):
        spark = MagicMock()
        with patch.object(data_store, "_list_iceberg_catalogs", return_value=[]):
            assert _list_iceberg_namespaces(spark) == []


# =============================================================================
# _list_hive_databases
# =============================================================================


class TestListHiveDatabases:
    """Tests for _list_hive_databases (direct HMS Thrift, no Spark)."""

    def test_returns_sorted_database_names(self):
        with patch.object(data_store.hive_metastore, "get_databases", return_value=["zeta", "alpha", "m"]):
            result = _list_hive_databases()
        assert result == ["alpha", "m", "zeta"]

    def test_returns_empty_on_failure(self):
        with patch.object(data_store.hive_metastore, "get_databases", side_effect=Exception("HMS down")):
            result = _list_hive_databases()
        assert result == []


# =============================================================================
# _aliases_for_user
# =============================================================================


class TestAliasesForUser:
    """Tests for _aliases_for_user."""

    def test_personal_aliases_includes_my_and_stripped(self):
        personal, tenant = _aliases_for_user({"POLARIS_PERSONAL_CATALOG": "user_alice", "POLARIS_TENANT_CATALOGS": ""})
        assert personal == {"my", "alice"}
        assert tenant == set()

    def test_personal_aliases_handles_no_personal_catalog(self):
        personal, tenant = _aliases_for_user({"POLARIS_PERSONAL_CATALOG": None, "POLARIS_TENANT_CATALOGS": None})
        assert personal == set()
        assert tenant == set()

    def test_tenant_aliases_strips_tenant_prefix(self):
        personal, tenant = _aliases_for_user(
            {
                "POLARIS_PERSONAL_CATALOG": "user_tgu2",
                "POLARIS_TENANT_CATALOGS": "tenant_globalusers,tenant_kbase",
            }
        )
        assert personal == {"my", "tgu2"}
        assert tenant == {"globalusers", "kbase"}

    def test_tenant_aliases_skips_blank_entries(self):
        personal, tenant = _aliases_for_user(
            {
                "POLARIS_PERSONAL_CATALOG": "user_tgu2",
                "POLARIS_TENANT_CATALOGS": "tenant_globalusers,,  ,tenant_kbase",
            }
        )
        assert tenant == {"globalusers", "kbase"}

    def test_accepts_object_with_attrs(self):
        ns = MagicMock()
        ns.POLARIS_PERSONAL_CATALOG = "user_bob"
        ns.POLARIS_TENANT_CATALOGS = "tenant_team1"

        personal, tenant = _aliases_for_user(ns)
        assert personal == {"my", "bob"}
        assert tenant == {"team1"}


# =============================================================================
# _filter_to_user_namespaces
# =============================================================================


class TestFilterToUserNamespaces:
    """Tests for _filter_to_user_namespaces."""

    def test_keeps_personal_iceberg_catalog(self):
        result = _filter_to_user_namespaces(
            ["my.demo", "stranger.foo"],
            username="alice",
            personal_aliases={"my", "alice"},
            tenant_aliases=set(),
        )
        assert result == ["my.demo"]

    def test_keeps_tenant_iceberg_catalog(self):
        result = _filter_to_user_namespaces(
            ["globalusers.shared", "stranger.foo"],
            username="alice",
            personal_aliases={"my", "alice"},
            tenant_aliases={"globalusers"},
        )
        assert result == ["globalusers.shared"]

    def test_keeps_user_hive_prefix(self):
        result = _filter_to_user_namespaces(
            ["u_alice__demo", "u_bob__demo"],
            username="alice",
            personal_aliases={"my", "alice"},
            tenant_aliases=set(),
        )
        assert result == ["u_alice__demo"]

    def test_keeps_tenant_hive_prefix(self):
        result = _filter_to_user_namespaces(
            ["globalusers_shared", "kbase_data", "u_other__db"],
            username="alice",
            personal_aliases={"my", "alice"},
            tenant_aliases={"globalusers"},
        )
        assert result == ["globalusers_shared"]

    def test_drops_unrelated_databases(self):
        result = _filter_to_user_namespaces(
            ["default", "u_bob__demo", "stranger.foo", "kbase_data"],
            username="alice",
            personal_aliases={"my", "alice"},
            tenant_aliases={"globalusers"},
        )
        assert result == []


# =============================================================================
# get_databases
# =============================================================================


class TestGetDatabases:
    """Tests for get_databases."""

    def test_returns_iceberg_and_hive_unfiltered(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=["my.demo", "kbase.shared"]),
            patch.object(data_store, "_list_hive_databases", return_value=["u_alice__demo", "default"]),
        ):
            result = get_databases(spark=spark, return_json=False, filter_by_namespace=False)

        assert result == ["default", "kbase.shared", "my.demo", "u_alice__demo"]

    def test_returns_json(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=["my.demo"]),
            patch.object(data_store, "_list_hive_databases", return_value=[]),
        ):
            result = get_databases(spark=spark, return_json=True, filter_by_namespace=False)

        assert json.loads(result) == ["my.demo"]

    def test_filter_by_namespace_keeps_personal_and_tenant(self):
        """End-to-end: settings drive both Iceberg catalog and Hive prefix filtering."""
        spark = MagicMock()
        with (
            patch.object(
                data_store,
                "_list_iceberg_namespaces",
                return_value=[
                    "my.demo_personal",
                    "tgu2.demo_personal",
                    "globalusers.shared_data",
                    "stranger.foo",
                ],
            ),
            patch.object(
                data_store,
                "_list_hive_databases",
                return_value=[
                    "default",
                    "globalusers_demo_shared",
                    "u_bsadkhin__demo",
                    "u_tgu2__demo_personal",
                ],
            ),
        ):
            result = get_databases(
                spark=spark,
                return_json=False,
                filter_by_namespace=True,
                settings={
                    "USER": "tgu2",
                    "POLARIS_PERSONAL_CATALOG": "user_tgu2",
                    "POLARIS_TENANT_CATALOGS": "tenant_globalusers",
                },
            )

        assert result == [
            "globalusers.shared_data",
            "globalusers_demo_shared",
            "my.demo_personal",
            "tgu2.demo_personal",
            "u_tgu2__demo_personal",
        ]

    def test_filter_requires_username(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=[]),
            patch.object(data_store, "_list_hive_databases", return_value=[]),
        ):
            with pytest.raises(ValueError, match="settings.USER"):
                get_databases(
                    spark=spark,
                    return_json=False,
                    filter_by_namespace=True,
                    settings={"USER": ""},
                )

    def test_uses_get_settings_default(self):
        """When settings is None, get_databases falls back to get_settings()."""
        spark = MagicMock()
        fake_settings = MagicMock()
        fake_settings.USER = "alice"
        fake_settings.POLARIS_PERSONAL_CATALOG = "user_alice"
        fake_settings.POLARIS_TENANT_CATALOGS = ""

        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=["my.demo"]),
            patch.object(data_store, "_list_hive_databases", return_value=["u_alice__db"]),
            patch.object(data_store, "get_settings", return_value=fake_settings),
        ):
            result = get_databases(spark=spark, return_json=False)

        assert result == ["my.demo", "u_alice__db"]

    def test_creates_spark_session_when_not_provided(self):
        """When spark is None, a session is created via get_spark_session()."""
        fake_spark = MagicMock()
        with (
            patch.object(data_store, "get_spark_session", return_value=fake_spark) as mock_get,
            patch.object(data_store, "_list_iceberg_namespaces", return_value=[]),
            patch.object(data_store, "_list_hive_databases", return_value=[]),
        ):
            get_databases(spark=None, return_json=False, filter_by_namespace=False)

        mock_get.assert_called_once()

    def test_use_hms_param_is_ignored(self):
        """use_hms is accepted for backward compatibility but does not affect behavior."""
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=["my.demo"]),
            patch.object(data_store, "_list_hive_databases", return_value=["u_alice__db"]),
        ):
            result_true = get_databases(spark=spark, use_hms=True, return_json=False, filter_by_namespace=False)
            result_false = get_databases(spark=spark, use_hms=False, return_json=False, filter_by_namespace=False)

        assert result_true == ["my.demo", "u_alice__db"]
        assert result_false == ["my.demo", "u_alice__db"]


class TestGetDatabasesByTenant:
    """Tests for get_databases with the tenant= parameter."""

    def test_single_tenant_filters_hive_and_iceberg(self):
        spark = MagicMock()
        with (
            patch.object(
                data_store,
                "_list_iceberg_namespaces",
                return_value=["globalusers.shared", "kbase.foo", "my.demo"],
            ),
            patch.object(
                data_store,
                "_list_hive_databases",
                return_value=[
                    "globalusers_analytics",
                    "globalusers_shared",
                    "kbase_data",
                    "u_alice__db",
                ],
            ),
        ):
            result = get_databases(spark=spark, tenant="globalusers", return_json=False)

        assert result == [
            "globalusers.shared",
            "globalusers_analytics",
            "globalusers_shared",
        ]

    def test_single_tenant_no_matches(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=["my.demo"]),
            patch.object(data_store, "_list_hive_databases", return_value=["u_alice__db"]),
        ):
            result = get_databases(spark=spark, tenant="teamx", return_json=False)
        assert result == []

    def test_single_tenant_returns_json(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "_list_iceberg_namespaces", return_value=[]),
            patch.object(data_store, "_list_hive_databases", return_value=["globalusers_db1"]),
        ):
            result = get_databases(spark=spark, tenant="globalusers", return_json=True)
        assert json.loads(result) == ["globalusers_db1"]


# =============================================================================
# get_tables
# =============================================================================


class TestGetTables:
    """Tests for get_tables — unified path via spark.catalog.listTables."""

    @staticmethod
    def _make_table(name: str):
        t = Mock()
        t.name = name
        return t

    def test_iceberg_uses_list_tables(self):
        spark = MagicMock()
        spark.catalog.listTables.return_value = [
            self._make_table("table_b"),
            self._make_table("table_a"),
        ]

        result = get_tables("my.demo", spark=spark, return_json=False)

        assert result == ["table_a", "table_b"]
        spark.catalog.listTables.assert_called_once_with(dbName="my.demo")

    def test_hive_uses_list_tables(self):
        spark = MagicMock()
        spark.catalog.listTables.return_value = [
            self._make_table("t2"),
            self._make_table("t1"),
        ]

        result = get_tables("u_alice__demo", spark=spark, return_json=False)

        assert result == ["t1", "t2"]
        spark.catalog.listTables.assert_called_once_with(dbName="u_alice__demo")

    def test_returns_empty_on_failure(self):
        spark = MagicMock()
        spark.catalog.listTables.side_effect = Exception("namespace not found")

        result = get_tables("my.missing", spark=spark, return_json=False)

        assert result == []

    def test_creates_spark_session_when_missing(self):
        fake_spark = MagicMock()
        fake_spark.catalog.listTables.return_value = []
        with patch.object(data_store, "get_spark_session", return_value=fake_spark) as mock_get:
            get_tables("my.demo", spark=None, return_json=False)
        mock_get.assert_called_once()

    def test_use_hms_param_is_ignored(self):
        """use_hms is accepted but listing always goes through Spark."""
        spark = MagicMock()
        spark.catalog.listTables.return_value = [self._make_table("t1")]

        result = get_tables("u_alice__db", use_hms=False, spark=spark, return_json=False)

        assert result == ["t1"]


# =============================================================================
# get_table_schema
# =============================================================================


class TestGetTableSchema:
    """Tests for get_table_schema."""

    def test_returns_column_names(self):
        spark = MagicMock()
        c1, c2 = Mock(name="id"), Mock(name="email")
        c1.name = "id"
        c2.name = "email"
        spark.catalog.listColumns.return_value = [c1, c2]

        result = get_table_schema("my.demo", "users", spark=spark, return_json=False)

        assert result == ["id", "email"]
        spark.catalog.listColumns.assert_called_once_with(tableName="my.demo.users")

    def test_works_for_hive_flat_database(self):
        spark = MagicMock()
        c1 = Mock()
        c1.name = "col1"
        spark.catalog.listColumns.return_value = [c1]

        result = get_table_schema("u_alice__demo", "t", spark=spark, return_json=False)

        assert result == ["col1"]
        spark.catalog.listColumns.assert_called_once_with(tableName="u_alice__demo.t")

    def test_returns_empty_on_failure(self):
        spark = MagicMock()
        spark.catalog.listColumns.side_effect = Exception("table not found")

        result = get_table_schema("my.demo", "missing", spark=spark, return_json=False)

        assert result == []

    def test_detailed_returns_column_dicts(self):
        spark = MagicMock()
        c1, c2 = Mock(), Mock()
        c1._asdict.return_value = {"name": "id", "dataType": "int"}
        c2._asdict.return_value = {"name": "email", "dataType": "string"}
        spark.catalog.listColumns.return_value = [c1, c2]

        result = get_table_schema("my.demo", "users", spark=spark, return_json=False, detailed=True)

        assert result == [{"name": "id", "dataType": "int"}, {"name": "email", "dataType": "string"}]

    def test_returns_json(self):
        spark = MagicMock()
        c1 = Mock()
        c1.name = "id"
        spark.catalog.listColumns.return_value = [c1]

        result = get_table_schema("my.demo", "t", spark=spark, return_json=True)
        assert json.loads(result) == ["id"]


# =============================================================================
# get_db_structure
# =============================================================================


class TestGetDbStructure:
    """Tests for get_db_structure."""

    def test_without_schema(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "get_databases", return_value=["my.demo", "u_alice__db"]),
            patch.object(data_store, "get_tables", side_effect=[["t1", "t2"], ["t3"]]),
        ):
            result = get_db_structure(with_schema=False, return_json=False, spark=spark, filter_by_namespace=False)

        assert result == {"my.demo": ["t1", "t2"], "u_alice__db": ["t3"]}

    def test_with_schema(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "get_databases", return_value=["my.demo"]),
            patch.object(data_store, "get_tables", return_value=["t1"]),
            patch.object(data_store, "get_table_schema", return_value=["c1", "c2"]),
        ):
            result = get_db_structure(with_schema=True, return_json=False, spark=spark, filter_by_namespace=False)

        assert result == {"my.demo": {"t1": ["c1", "c2"]}}

    def test_returns_json(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "get_databases", return_value=["my.demo"]),
            patch.object(data_store, "get_tables", return_value=["t1"]),
        ):
            result = get_db_structure(with_schema=False, return_json=True, spark=spark, filter_by_namespace=False)

        assert json.loads(result) == {"my.demo": ["t1"]}

    def test_creates_spark_session_when_missing(self):
        fake_spark = MagicMock()
        with (
            patch.object(data_store, "get_spark_session", return_value=fake_spark) as mock_get,
            patch.object(data_store, "get_databases", return_value=[]),
        ):
            get_db_structure(spark=None, return_json=False, filter_by_namespace=False)
        mock_get.assert_called_once()


# =============================================================================
# Caching behavior
# =============================================================================


class TestCaching:
    """Tests for the in-process TTL caches added to the public API."""

    @staticmethod
    def _make_table(name: str):
        t = Mock()
        t.name = name
        return t

    def test_get_tables_cache_hit_skips_spark(self):
        spark = MagicMock()
        spark.catalog.listTables.return_value = [self._make_table("t1"), self._make_table("t2")]

        first = get_tables("my.demo", spark=spark, return_json=False)
        second = get_tables("my.demo", spark=spark, return_json=False)

        assert first == second == ["t1", "t2"]
        # Underlying Spark call happens only once.
        spark.catalog.listTables.assert_called_once_with(dbName="my.demo")

    def test_get_tables_force_refresh_bypasses_cache(self):
        spark = MagicMock()
        spark.catalog.listTables.side_effect = [
            [self._make_table("t1")],
            [self._make_table("t1"), self._make_table("t2")],
        ]

        first = get_tables("my.demo", spark=spark, return_json=False)
        second = get_tables("my.demo", spark=spark, return_json=False, force_refresh=True)

        assert first == ["t1"]
        assert second == ["t1", "t2"]
        assert spark.catalog.listTables.call_count == 2

    def test_get_tables_invalidate_cache_clears_entry(self):
        spark = MagicMock()
        spark.catalog.listTables.side_effect = [
            [self._make_table("t1")],
            [self._make_table("t1"), self._make_table("t2")],
        ]

        first = get_tables("my.demo", spark=spark, return_json=False)
        invalidate_cache()
        second = get_tables("my.demo", spark=spark, return_json=False)

        assert first == ["t1"]
        assert second == ["t1", "t2"]
        assert spark.catalog.listTables.call_count == 2

    def test_get_tables_cache_returns_json_from_raw_cache(self):
        """Cache stores raw list; return_json formats on retrieval."""
        spark = MagicMock()
        spark.catalog.listTables.return_value = [self._make_table("t1")]

        raw = get_tables("my.demo", spark=spark, return_json=False)
        as_json = get_tables("my.demo", spark=spark, return_json=True)

        assert raw == ["t1"]
        assert json.loads(as_json) == ["t1"]
        spark.catalog.listTables.assert_called_once()

    def test_get_table_schema_cache_hit(self):
        spark = MagicMock()
        c1 = Mock()
        c1.name = "id"
        c2 = Mock()
        c2.name = "email"
        spark.catalog.listColumns.return_value = [c1, c2]

        first = get_table_schema("my.demo", "users", spark=spark, return_json=False)
        second = get_table_schema("my.demo", "users", spark=spark, return_json=False)

        assert first == second == ["id", "email"]
        spark.catalog.listColumns.assert_called_once()

    def test_get_table_schema_detailed_keys_separately(self):
        """detailed=True / detailed=False are separate cache entries."""
        spark = MagicMock()
        c1 = Mock()
        c1.name = "id"
        c1._asdict.return_value = {"name": "id", "dataType": "int"}
        spark.catalog.listColumns.return_value = [c1]

        names = get_table_schema("my.demo", "users", spark=spark, return_json=False, detailed=False)
        details = get_table_schema("my.demo", "users", spark=spark, return_json=False, detailed=True)

        assert names == ["id"]
        assert details == [{"name": "id", "dataType": "int"}]
        # Two distinct cache keys → two real lookups.
        assert spark.catalog.listColumns.call_count == 2

    def test_get_databases_cache_hit_skips_spark_and_hms(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = _make_set_rows()
        settings = {"USER": "alice"}
        with patch.object(data_store.hive_metastore, "get_databases", return_value=["u_alice__demo"]) as mock_hms:
            first = get_databases(spark=spark, return_json=False, filter_by_namespace=False, settings=settings)
            second = get_databases(spark=spark, return_json=False, filter_by_namespace=False, settings=settings)

        assert first == second == ["u_alice__demo"]
        mock_hms.assert_called_once()

    def test_get_databases_cache_keyed_by_tenant(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = _make_set_rows()
        settings = {"USER": "alice"}
        hive_dbs = ["kbase_demo", "u_alice__demo"]
        with patch.object(data_store.hive_metastore, "get_databases", return_value=hive_dbs) as mock_hms:
            tenant_view = get_databases(
                spark=spark,
                return_json=False,
                filter_by_namespace=False,
                tenant="kbase",
                settings=settings,
            )
            user_view = get_databases(spark=spark, return_json=False, filter_by_namespace=False, settings=settings)

        assert tenant_view == ["kbase_demo"]
        assert user_view == ["kbase_demo", "u_alice__demo"]
        # Two different cache keys → two real lookups.
        assert mock_hms.call_count == 2

    def test_get_db_structure_cache_hit(self):
        spark = MagicMock()
        with (
            patch.object(data_store, "get_databases", return_value=["my.demo"]) as mock_dbs,
            patch.object(data_store, "get_tables", return_value=["t1"]) as mock_tables,
        ):
            first = get_db_structure(with_schema=False, return_json=False, spark=spark, filter_by_namespace=False)
            second = get_db_structure(with_schema=False, return_json=False, spark=spark, filter_by_namespace=False)

        assert first == second == {"my.demo": ["t1"]}
        # Inner builders run only on the first call.
        mock_dbs.assert_called_once()
        mock_tables.assert_called_once()

    def test_get_db_structure_force_refresh_propagates(self):
        """force_refresh on the structure should propagate to inner calls."""
        spark = MagicMock()
        with (
            patch.object(data_store, "get_databases", return_value=["my.demo"]) as mock_dbs,
            patch.object(data_store, "get_tables", return_value=["t1"]) as mock_tables,
        ):
            get_db_structure(with_schema=False, return_json=False, spark=spark, filter_by_namespace=False)
            get_db_structure(
                with_schema=False, return_json=False, spark=spark, filter_by_namespace=False, force_refresh=True
            )

        assert mock_dbs.call_count == 2
        # And the second call to get_databases must have force_refresh=True.
        assert mock_dbs.call_args_list[1].kwargs.get("force_refresh") is True
        assert mock_tables.call_args_list[1].kwargs.get("force_refresh") is True
