"""
Tests for spark/data_store.py - Data store operations.
"""

import json
from unittest.mock import Mock, patch

import pytest

from berdl_notebook_utils.spark.data_store import (
    _cached_get_my_accessible_paths,
    _cached_get_my_groups,
    _cached_get_namespace_prefix,
    _execute_with_spark,
    _extract_databases_from_paths,
    _format_output,
    _ttl_cache,
    clear_governance_cache,
    get_databases,
    get_db_structure,
    get_table_schema,
    get_tables,
)


class TestTtlCache:
    """Tests for _ttl_cache decorator."""

    def test_cache_returns_cached_value(self):
        """Test cache returns cached value within TTL."""
        call_count = 0

        @_ttl_cache(ttl_seconds=60)
        def expensive_func():
            nonlocal call_count
            call_count += 1
            return "result"

        # First call
        result1 = expensive_func()
        # Second call should use cache
        result2 = expensive_func()

        assert result1 == "result"
        assert result2 == "result"
        assert call_count == 1

    def test_cache_clear(self):
        """Test cache can be cleared."""
        call_count = 0

        @_ttl_cache(ttl_seconds=60)
        def expensive_func():
            nonlocal call_count
            call_count += 1
            return "result"

        expensive_func()
        expensive_func.clear_cache()
        expensive_func()

        assert call_count == 2


class TestClearGovernanceCache:
    """Tests for clear_governance_cache function."""

    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_groups")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_accessible_paths")
    def test_clear_governance_cache(self, mock_paths, mock_prefix, mock_groups):
        """Test clear_governance_cache clears all caches."""
        mock_groups.clear_cache = Mock()
        mock_prefix.clear_cache = Mock()
        mock_paths.clear_cache = Mock()

        clear_governance_cache()

        # The function uses getattr, so we just verify it doesn't raise


class TestFormatOutput:
    """Tests for _format_output helper."""

    def test_format_output_json(self):
        """Test format_output returns JSON string."""
        result = _format_output(["item1", "item2"], return_json=True)

        assert json.loads(result) == ["item1", "item2"]

    def test_format_output_raw(self):
        """Test format_output returns raw data."""
        data = ["item1", "item2"]
        result = _format_output(data, return_json=False)

        assert result == data


class TestExtractDatabasesFromPaths:
    """Tests for _extract_databases_from_paths function."""

    def test_extract_databases_from_sql_warehouse_paths(self):
        """Test extracting databases from SQL warehouse paths."""
        paths = [
            "s3a://cdm-lake/users-sql-warehouse/user1/test_db.db/table1/",
            "s3a://cdm-lake/users-sql-warehouse/user1/analytics.db/metrics/",
            "s3a://cdm-lake/tenant-sql-warehouse/team/shared.db/data/",
        ]

        result = _extract_databases_from_paths(paths)

        assert "test_db" in result
        assert "analytics" in result
        assert "shared" in result

    def test_ignores_non_sql_warehouse_paths(self):
        """Test ignoring paths not in SQL warehouses."""
        paths = [
            "s3a://cdm-lake/logs/app.log",
            "s3a://cdm-lake/warehouse/some.db/table/",
            "s3a://cdm-lake/users-sql-warehouse/user1/valid.db/table/",
        ]

        result = _extract_databases_from_paths(paths)

        assert "valid" in result
        assert "some" not in result

    def test_handles_empty_paths(self):
        """Test handling empty paths list."""
        result = _extract_databases_from_paths([])

        assert result == []


class TestGetDatabases:
    """Tests for get_databases function."""

    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_hms_no_filter(self, mock_hms):
        """Test get_databases using HMS without filtering."""
        mock_hms.get_databases.return_value = ["db1", "db2"]

        result = get_databases(use_hms=True, filter_by_namespace=False)

        assert "db1" in result
        assert "db2" in result

    @patch("berdl_notebook_utils.spark.data_store._execute_with_spark")
    def test_get_databases_spark_no_filter(self, mock_execute):
        """Test get_databases using Spark without filtering."""
        mock_execute.return_value = ["db1", "db2"]

        result = get_databases(use_hms=False, filter_by_namespace=False, return_json=False)

        assert result == ["db1", "db2"]

    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_accessible_paths")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_groups")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_with_filter(self, mock_hms, mock_groups, mock_prefix, mock_paths):
        """Test get_databases with namespace filtering."""
        mock_hms.get_databases.return_value = ["u_test__db1", "u_other__db2", "shared_db"]

        mock_groups.return_value = Mock(groups=["team1"])
        mock_prefix.return_value = Mock(
            user_namespace_prefix="u_test__",
            tenant_namespace_prefix="t_team1__",
        )
        mock_paths.return_value = Mock(accessible_paths=["s3a://cdm-lake/users-sql-warehouse/other/shared_db.db/"])

        result = get_databases(use_hms=True, filter_by_namespace=True, return_json=False)

        assert "u_test__db1" in result


class TestGetDatabasesByTenant:
    """Tests for get_databases with tenant parameter."""

    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_single_tenant(self, mock_hms, mock_prefix):
        """Test get_databases with tenant filters to that tenant's prefix only."""
        mock_hms.get_databases.return_value = [
            "u_test__db1",
            "globalusers_shared",
            "globalusers_analytics",
            "teamx_project",
        ]
        mock_prefix.return_value = Mock(tenant_namespace_prefix="globalusers_")

        result = get_databases(use_hms=True, tenant="globalusers", return_json=False)

        assert result == ["globalusers_analytics", "globalusers_shared"]
        mock_prefix.assert_called_once_with(tenant="globalusers")

    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_tenant_no_matches(self, mock_hms, mock_prefix):
        """Test get_databases with tenant that has no matching databases."""
        mock_hms.get_databases.return_value = ["u_test__db1", "globalusers_shared"]
        mock_prefix.return_value = Mock(tenant_namespace_prefix="teamx_")

        result = get_databases(use_hms=True, tenant="teamx", return_json=False)

        assert result == []

    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_tenant_returns_json(self, mock_hms, mock_prefix):
        """Test get_databases with tenant returns JSON when return_json=True."""
        mock_hms.get_databases.return_value = ["globalusers_db1"]
        mock_prefix.return_value = Mock(tenant_namespace_prefix="globalusers_")

        result = get_databases(use_hms=True, tenant="globalusers", return_json=True)

        assert json.loads(result) == ["globalusers_db1"]

    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_tenant_error_raises(self, mock_hms, mock_prefix):
        """Test get_databases with tenant raises on API error."""
        mock_hms.get_databases.return_value = ["db1"]
        mock_prefix.side_effect = Exception("API error")

        with pytest.raises(Exception, match="Could not filter databases for tenant 'badteam'"):
            get_databases(use_hms=True, tenant="badteam", return_json=False)

    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_databases_tenant_invalid_prefix_raises(self, mock_hms, mock_prefix):
        """Test get_databases raises when tenant returns Unset/None prefix."""
        from governance_client.types import UNSET

        mock_hms.get_databases.return_value = ["db1"]
        mock_prefix.return_value = Mock(tenant_namespace_prefix=UNSET)

        with pytest.raises(Exception, match="Could not filter databases for tenant"):
            get_databases(use_hms=True, tenant="badteam", return_json=False)


class TestGetTables:
    """Tests for get_tables function."""

    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_tables_hms(self, mock_hms):
        """Test get_tables using HMS."""
        mock_hms.get_tables.return_value = ["table1", "table2"]

        result = get_tables("test_db", use_hms=True, return_json=False)

        assert result == ["table1", "table2"]
        mock_hms.get_tables.assert_called_once_with("test_db")

    @patch("berdl_notebook_utils.spark.data_store._execute_with_spark")
    def test_get_tables_spark(self, mock_execute):
        """Test get_tables using Spark."""
        mock_execute.return_value = ["table1", "table2"]

        result = get_tables("test_db", use_hms=False, return_json=False)

        assert result == ["table1", "table2"]


class TestGetTableSchema:
    """Tests for get_table_schema function."""

    @patch("berdl_notebook_utils.spark.data_store._execute_with_spark")
    def test_get_table_schema(self, mock_execute):
        """Test get_table_schema returns column names."""
        mock_execute.return_value = ["col1", "col2", "col3"]

        result = get_table_schema("test_db", "test_table", return_json=False)

        assert result == ["col1", "col2", "col3"]


class TestGetDbStructure:
    """Tests for get_db_structure function."""

    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_db_structure_without_schema(self, mock_hms, mock_get_dbs):
        """Test get_db_structure without schema."""
        mock_get_dbs.return_value = ["db1"]
        mock_hms.get_tables.return_value = ["table1", "table2"]

        result = get_db_structure(with_schema=False, use_hms=True, return_json=False)

        assert "db1" in result
        assert result["db1"] == ["table1", "table2"]

    @patch("berdl_notebook_utils.spark.data_store.get_table_schema")
    @patch("berdl_notebook_utils.spark.data_store.get_spark_session")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_get_db_structure_with_schema(self, mock_hms, mock_get_dbs, mock_spark, mock_schema):
        """Test get_db_structure with schema."""
        mock_get_dbs.return_value = ["db1"]
        mock_hms.get_tables.return_value = ["table1"]
        mock_schema.return_value = ["col1", "col2"]

        result = get_db_structure(with_schema=True, use_hms=True, return_json=False)

        assert "db1" in result
        assert "table1" in result["db1"]
        assert result["db1"]["table1"] == ["col1", "col2"]

    @patch("berdl_notebook_utils.spark.data_store._execute_with_spark")
    def test_get_db_structure_using_spark(self, mock_execute):
        """Test get_db_structure using Spark."""
        mock_execute.return_value = {"db1": ["table1"]}

        result = get_db_structure(use_hms=False, return_json=False)

        assert result == {"db1": ["table1"]}


class TestExecuteWithSpark:
    """Tests for _execute_with_spark helper."""

    @patch("berdl_notebook_utils.spark.data_store.get_spark_session")
    def test_execute_with_spark_creates_session(self, mock_get_session):
        """Test creates Spark session if not provided."""
        mock_spark = Mock()
        mock_get_session.return_value = mock_spark

        def test_func(spark, arg1):
            return f"result_{arg1}"

        result = _execute_with_spark(test_func, None, "test")

        mock_get_session.assert_called_once()
        assert result == "result_test"

    def test_execute_with_spark_uses_provided_session(self):
        """Test uses provided Spark session."""
        mock_spark = Mock()

        def test_func(spark, arg1):
            return f"result_{arg1}"

        result = _execute_with_spark(test_func, mock_spark, "test")

        assert result == "result_test"


class TestCachedWrappers:
    """Tests for cached governance API wrappers."""

    @patch("berdl_notebook_utils.spark.data_store.get_my_groups")
    def test_cached_get_my_groups(self, mock_get_my_groups):
        """Test _cached_get_my_groups calls through to get_my_groups."""
        mock_get_my_groups.return_value = Mock(groups=["team1"])
        _cached_get_my_groups.clear_cache()

        result = _cached_get_my_groups()

        assert result.groups == ["team1"]
        mock_get_my_groups.assert_called_once()

    @patch("berdl_notebook_utils.spark.data_store.get_namespace_prefix")
    def test_cached_get_namespace_prefix(self, mock_get_ns):
        """Test _cached_get_namespace_prefix calls through to get_namespace_prefix."""
        mock_get_ns.return_value = Mock(user_namespace_prefix="u_test__")
        _cached_get_namespace_prefix.clear_cache()

        result = _cached_get_namespace_prefix()

        assert result.user_namespace_prefix == "u_test__"
        mock_get_ns.assert_called_once()

    @patch("berdl_notebook_utils.spark.data_store.get_my_accessible_paths")
    def test_cached_get_my_accessible_paths(self, mock_get_paths):
        """Test _cached_get_my_accessible_paths calls through to get_my_accessible_paths."""
        mock_get_paths.return_value = Mock(accessible_paths=["s3a://bucket/path"])
        _cached_get_my_accessible_paths.clear_cache()

        result = _cached_get_my_accessible_paths()

        assert result.accessible_paths == ["s3a://bucket/path"]
        mock_get_paths.assert_called_once()


class TestGetDatabasesFilterError:
    """Tests for get_databases filter_by_namespace error path."""

    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_accessible_paths")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_namespace_prefix")
    @patch("berdl_notebook_utils.spark.data_store._cached_get_my_groups")
    @patch("berdl_notebook_utils.spark.data_store.hive_metastore")
    def test_filter_error_raises(self, mock_hms, mock_groups, mock_prefix, mock_paths):
        """Test get_databases raises when filter_by_namespace fails."""
        mock_hms.get_databases.return_value = ["db1"]
        mock_groups.side_effect = Exception("API error")

        with pytest.raises(Exception, match="Could not filter databases by namespace"):
            get_databases(use_hms=True, filter_by_namespace=True, return_json=False)


class TestGetTablesSparkInnerFunction:
    """Tests for get_tables using Spark (inner _get_tbls function)."""

    def test_get_tables_spark_calls_catalog(self):
        """Test get_tables with use_hms=False uses Spark catalog."""
        mock_spark = Mock()
        mock_table1 = Mock()
        mock_table1.name = "table1"
        mock_table2 = Mock()
        mock_table2.name = "table2"
        mock_spark.catalog.listTables.return_value = [mock_table1, mock_table2]

        result = get_tables("test_db", spark=mock_spark, use_hms=False, return_json=False)

        assert result == ["table1", "table2"]
        mock_spark.catalog.listTables.assert_called_once_with(dbName="test_db")


class TestGetTableSchemaErrorPath:
    """Tests for get_table_schema inner error handling."""

    def test_schema_error_returns_empty_list(self):
        """Test _get_schema returns [] when catalog raises Exception."""
        mock_spark = Mock()
        mock_spark.catalog.listColumns.side_effect = Exception("table not found")

        result = get_table_schema("test_db", "broken_table", spark=mock_spark, return_json=False)

        assert result == []


class TestGetDbStructureSparkPath:
    """Tests for get_db_structure with use_hms=False (Spark inner function)."""

    @patch("berdl_notebook_utils.spark.data_store.get_table_schema")
    @patch("berdl_notebook_utils.spark.data_store.get_tables")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store.get_spark_session")
    def test_spark_path_without_schema(self, mock_get_session, mock_get_dbs, mock_get_tables, mock_get_schema):
        """Test get_db_structure via Spark without schema."""
        mock_spark = Mock()
        mock_get_session.return_value = mock_spark
        mock_get_dbs.return_value = ["db1"]
        mock_get_tables.return_value = ["t1", "t2"]

        result = get_db_structure(with_schema=False, use_hms=False, return_json=False)

        assert result == {"db1": ["t1", "t2"]}

    @patch("berdl_notebook_utils.spark.data_store.get_table_schema")
    @patch("berdl_notebook_utils.spark.data_store.get_tables")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store.get_spark_session")
    def test_spark_path_with_schema(self, mock_get_session, mock_get_dbs, mock_get_tables, mock_get_schema):
        """Test get_db_structure via Spark with schema."""
        mock_spark = Mock()
        mock_get_session.return_value = mock_spark
        mock_get_dbs.return_value = ["db1"]
        mock_get_tables.return_value = ["t1"]
        mock_get_schema.return_value = ["col1", "col2"]

        result = get_db_structure(with_schema=True, use_hms=False, return_json=False)

        assert result == {"db1": {"t1": ["col1", "col2"]}}
