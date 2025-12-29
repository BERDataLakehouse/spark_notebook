"""
Extended tests for spark/database.py to increase coverage.

Tests for: table_exists, remove_table, list_tables, list_namespaces, get_table_info
"""

from unittest.mock import Mock, MagicMock
import pytest

from berdl_notebook_utils.spark.database import (
    table_exists,
    remove_table,
    list_tables,
    list_namespaces,
    get_table_info,
    DEFAULT_NAMESPACE,
)


def make_mock_spark() -> Mock:
    """Create a mock SparkSession with catalog."""
    mock_spark = Mock(name="SparkSession")
    mock_catalog = Mock(name="Catalog")
    mock_spark.catalog = mock_catalog
    return mock_spark


class TestTableExists:
    """Tests for table_exists function."""

    def test_table_exists_returns_true(self, capfd: pytest.CaptureFixture[str]):
        """Test table_exists returns True when table exists."""
        mock_spark = make_mock_spark()
        mock_spark.catalog.tableExists.return_value = True

        result = table_exists(mock_spark, "my_table", "my_namespace")

        assert result is True
        mock_spark.catalog.tableExists.assert_called_once_with("my_namespace.my_table")
        captured = capfd.readouterr()
        assert "my_namespace.my_table exists" in captured.out

    def test_table_exists_returns_false(self, capfd: pytest.CaptureFixture[str]):
        """Test table_exists returns False when table doesn't exist."""
        mock_spark = make_mock_spark()
        mock_spark.catalog.tableExists.return_value = False

        result = table_exists(mock_spark, "missing_table", "my_namespace")

        assert result is False
        mock_spark.catalog.tableExists.assert_called_once_with("my_namespace.missing_table")
        captured = capfd.readouterr()
        assert "does not exist" in captured.out

    def test_table_exists_uses_default_namespace(self):
        """Test table_exists uses default namespace when not specified."""
        mock_spark = make_mock_spark()
        mock_spark.catalog.tableExists.return_value = True

        table_exists(mock_spark, "my_table")

        mock_spark.catalog.tableExists.assert_called_once_with(f"{DEFAULT_NAMESPACE}.my_table")


class TestRemoveTable:
    """Tests for remove_table function."""

    def test_remove_table_drops_table(self, capfd: pytest.CaptureFixture[str]):
        """Test remove_table executes DROP TABLE SQL."""
        mock_spark = make_mock_spark()

        remove_table(mock_spark, "my_table", "my_namespace")

        mock_spark.sql.assert_called_once_with("DROP TABLE IF EXISTS my_namespace.my_table")
        captured = capfd.readouterr()
        assert "my_namespace.my_table removed" in captured.out

    def test_remove_table_uses_default_namespace(self):
        """Test remove_table uses default namespace when not specified."""
        mock_spark = make_mock_spark()

        remove_table(mock_spark, "my_table")

        mock_spark.sql.assert_called_once_with(f"DROP TABLE IF EXISTS {DEFAULT_NAMESPACE}.my_table")


class TestListTables:
    """Tests for list_tables function."""

    def test_list_tables_in_namespace(self):
        """Test list_tables returns tables from specific namespace."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [
            {"tableName": "table1"},
            {"tableName": "table2"},
            {"tableName": "table3"},
        ]
        mock_spark.sql.return_value = mock_df

        result = list_tables(mock_spark, "my_namespace")

        assert result == ["table1", "table2", "table3"]
        mock_spark.sql.assert_called_once_with("SHOW TABLES IN my_namespace")

    def test_list_tables_without_namespace(self):
        """Test list_tables without namespace uses current namespace."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [{"tableName": "table1"}]
        mock_spark.sql.return_value = mock_df

        result = list_tables(mock_spark)

        assert result == ["table1"]
        mock_spark.sql.assert_called_once_with("SHOW TABLES")

    def test_list_tables_handles_error(self, capfd: pytest.CaptureFixture[str]):
        """Test list_tables returns empty list on error."""
        mock_spark = make_mock_spark()
        mock_spark.sql.side_effect = Exception("Database not found")

        result = list_tables(mock_spark, "nonexistent_namespace")

        assert result == []
        captured = capfd.readouterr()
        assert "Error listing tables" in captured.out

    def test_list_tables_empty_namespace(self):
        """Test list_tables returns empty list for namespace with no tables."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        result = list_tables(mock_spark, "empty_namespace")

        assert result == []


class TestListNamespaces:
    """Tests for list_namespaces function."""

    def test_list_namespaces_returns_databases(self):
        """Test list_namespaces returns all databases."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [
            {"namespace": "default"},
            {"namespace": "analytics"},
            {"namespace": "user__experiments"},
        ]
        mock_spark.sql.return_value = mock_df

        result = list_namespaces(mock_spark)

        assert result == ["default", "analytics", "user__experiments"]
        mock_spark.sql.assert_called_once_with("SHOW DATABASES")

    def test_list_namespaces_handles_error(self, capfd: pytest.CaptureFixture[str]):
        """Test list_namespaces returns empty list on error."""
        mock_spark = make_mock_spark()
        mock_spark.sql.side_effect = Exception("Connection failed")

        result = list_namespaces(mock_spark)

        assert result == []
        captured = capfd.readouterr()
        assert "Error listing namespaces" in captured.out

    def test_list_namespaces_empty(self):
        """Test list_namespaces returns empty list when no databases exist."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        result = list_namespaces(mock_spark)

        assert result == []


class TestGetTableInfo:
    """Tests for get_table_info function."""

    def test_get_table_info_returns_schema(self):
        """Test get_table_info returns table schema information."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [
            {"col_name": "id", "data_type": "int"},
            {"col_name": "name", "data_type": "string"},
            {"col_name": "created_at", "data_type": "timestamp"},
        ]
        mock_spark.sql.return_value = mock_df

        result = get_table_info(mock_spark, "my_table", "my_namespace")

        assert result == {
            "id": "int",
            "name": "string",
            "created_at": "timestamp",
        }
        mock_spark.sql.assert_called_once_with("DESCRIBE EXTENDED my_namespace.my_table")

    def test_get_table_info_uses_default_namespace(self):
        """Test get_table_info uses default namespace when not specified."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [{"col_name": "id", "data_type": "int"}]
        mock_spark.sql.return_value = mock_df

        get_table_info(mock_spark, "my_table")

        mock_spark.sql.assert_called_once_with(f"DESCRIBE EXTENDED {DEFAULT_NAMESPACE}.my_table")

    def test_get_table_info_handles_error(self, capfd: pytest.CaptureFixture[str]):
        """Test get_table_info returns empty dict on error."""
        mock_spark = make_mock_spark()
        mock_spark.sql.side_effect = Exception("Table not found")

        result = get_table_info(mock_spark, "nonexistent_table", "my_namespace")

        assert result == {}
        captured = capfd.readouterr()
        assert "Error getting table info" in captured.out

    def test_get_table_info_skips_empty_rows(self):
        """Test get_table_info skips rows with empty values."""
        mock_spark = make_mock_spark()
        mock_df = Mock()
        mock_df.collect.return_value = [
            {"col_name": "id", "data_type": "int"},
            {"col_name": "", "data_type": ""},  # Empty row (partition info separator)
            {"col_name": None, "data_type": None},  # Null row
            {"col_name": "name", "data_type": "string"},
        ]
        mock_spark.sql.return_value = mock_df

        result = get_table_info(mock_spark, "my_table")

        # Should only include rows with non-empty values
        assert result == {"id": "int", "name": "string"}
