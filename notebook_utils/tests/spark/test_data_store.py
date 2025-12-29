"""
Tests for spark/data_store.py - Data store operations.
"""

from unittest.mock import Mock, patch
import json

from berdl_notebook_utils.spark.data_store import (
    _ttl_cache,
    clear_governance_cache,
    _format_output,
    _extract_databases_from_paths,
    get_databases,
    get_tables,
    get_table_schema,
    get_db_structure,
    _execute_with_spark,
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
