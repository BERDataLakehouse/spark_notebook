"""
Tests for spark/data_store.py - Iceberg catalog data store operations.
"""

import json
from unittest.mock import MagicMock, patch

from berdl_notebook_utils.spark.data_store import (
    _format_output,
    _list_iceberg_catalogs,
    get_databases,
    get_tables,
    get_table_schema,
    get_db_structure,
)


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

    def test_format_output_dict_json(self):
        """Test format_output with dict data."""
        data = {"db1": ["table1"]}
        result = _format_output(data, return_json=True)
        assert json.loads(result) == data


class TestListIcebergCatalogs:
    """Tests for _list_iceberg_catalogs."""

    def test_excludes_spark_catalog(self):
        """Test that spark_catalog is excluded."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"catalog": "spark_catalog"},
            {"catalog": "my"},
            {"catalog": "kbase"},
        ]

        result = _list_iceberg_catalogs(mock_spark)

        assert result == ["kbase", "my"]
        assert "spark_catalog" not in result

    def test_returns_sorted(self):
        """Test that catalogs are returned sorted."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = [
            {"catalog": "zebra"},
            {"catalog": "alpha"},
        ]

        result = _list_iceberg_catalogs(mock_spark)

        assert result == ["alpha", "zebra"]

    def test_empty_catalogs(self):
        """Test handling when no catalogs exist."""
        mock_spark = MagicMock()
        mock_spark.sql.return_value.collect.return_value = []

        result = _list_iceberg_catalogs(mock_spark)

        assert result == []


class TestGetDatabases:
    """Tests for get_databases function."""

    @patch("berdl_notebook_utils.spark.data_store._list_iceberg_catalogs")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_databases_returns_catalog_namespace_format(self, mock_get_spark, mock_list_catalogs):
        """Test that databases are returned in catalog.namespace format."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_list_catalogs.return_value = ["kbase", "my"]

        def sql_side_effect(query):
            result = MagicMock()
            if "SHOW NAMESPACES IN kbase" in query:
                result.collect.return_value = [{"namespace": "shared_data"}, {"namespace": "research"}]
            elif "SHOW NAMESPACES IN my" in query:
                result.collect.return_value = [{"namespace": "demo"}, {"namespace": "analysis"}]
            return result

        mock_spark.sql.side_effect = sql_side_effect

        result = get_databases(return_json=False)

        assert result == ["kbase.research", "kbase.shared_data", "my.analysis", "my.demo"]

    @patch("berdl_notebook_utils.spark.data_store._list_iceberg_catalogs")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_databases_returns_json(self, mock_get_spark, mock_list_catalogs):
        """Test get_databases returns JSON string."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_list_catalogs.return_value = ["my"]
        mock_spark.sql.return_value.collect.return_value = [{"namespace": "demo"}]

        result = get_databases(return_json=True)

        assert json.loads(result) == ["my.demo"]

    @patch("berdl_notebook_utils.spark.data_store._list_iceberg_catalogs")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_databases_handles_inaccessible_catalog(self, mock_get_spark, mock_list_catalogs):
        """Test that inaccessible catalogs are skipped."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_list_catalogs.return_value = ["my", "broken"]

        def sql_side_effect(query):
            if "broken" in query:
                raise Exception("Catalog not accessible")
            result = MagicMock()
            result.collect.return_value = [{"namespace": "demo"}]
            return result

        mock_spark.sql.side_effect = sql_side_effect

        result = get_databases(return_json=False)

        assert result == ["my.demo"]

    @patch("berdl_notebook_utils.spark.data_store._list_iceberg_catalogs")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_databases_empty(self, mock_get_spark, mock_list_catalogs):
        """Test get_databases with no catalogs."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_list_catalogs.return_value = []

        result = get_databases(return_json=False)

        assert result == []


class TestGetTables:
    """Tests for get_tables function."""

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_tables(self, mock_get_spark):
        """Test get_tables returns sorted table names."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value.collect.return_value = [
            {"tableName": "users"},
            {"tableName": "orders"},
        ]

        result = get_tables("my.demo", return_json=False)

        assert result == ["orders", "users"]
        mock_spark.sql.assert_called_with("SHOW TABLES IN my.demo")

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_tables_returns_json(self, mock_get_spark):
        """Test get_tables returns JSON string."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value.collect.return_value = [{"tableName": "t1"}]

        result = get_tables("my.demo", return_json=True)

        assert json.loads(result) == ["t1"]

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_tables_handles_error(self, mock_get_spark):
        """Test get_tables returns empty list on error."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.side_effect = Exception("Namespace not found")

        result = get_tables("my.nonexistent", return_json=False)

        assert result == []


class TestGetTableSchema:
    """Tests for get_table_schema function."""

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_table_schema(self, mock_get_spark):
        """Test get_table_schema returns column names."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value.collect.return_value = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "name", "data_type": "string", "comment": ""},
            {"col_name": "age", "data_type": "int", "comment": ""},
        ]

        result = get_table_schema("my.demo", "users", return_json=False)

        assert result == ["id", "name", "age"]
        mock_spark.sql.assert_called_with("DESCRIBE my.demo.users")

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_table_schema_filters_metadata_rows(self, mock_get_spark):
        """Test that partition/metadata rows starting with # are filtered."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.return_value.collect.return_value = [
            {"col_name": "id", "data_type": "int", "comment": ""},
            {"col_name": "# Partitioning", "data_type": "", "comment": ""},
            {"col_name": "# col_name", "data_type": "data_type", "comment": ""},
        ]

        result = get_table_schema("my.demo", "users", return_json=False)

        assert result == ["id"]

    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_table_schema_handles_error(self, mock_get_spark):
        """Test get_table_schema returns empty list on error."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_spark.sql.side_effect = Exception("Table not found")

        result = get_table_schema("my.demo", "nonexistent", return_json=False)

        assert result == []


class TestGetDbStructure:
    """Tests for get_db_structure function."""

    @patch("berdl_notebook_utils.spark.data_store.get_table_schema")
    @patch("berdl_notebook_utils.spark.data_store.get_tables")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_db_structure_without_schema(self, mock_get_spark, mock_get_dbs, mock_get_tables, mock_schema):
        """Test get_db_structure without schema."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_dbs.return_value = ["my.demo", "kbase.shared"]
        mock_get_tables.side_effect = lambda database, **kwargs: {
            "my.demo": ["table1", "table2"],
            "kbase.shared": ["dataset"],
        }[database]

        result = get_db_structure(with_schema=False, return_json=False)

        assert result == {
            "my.demo": ["table1", "table2"],
            "kbase.shared": ["dataset"],
        }

    @patch("berdl_notebook_utils.spark.data_store.get_table_schema")
    @patch("berdl_notebook_utils.spark.data_store.get_tables")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_db_structure_with_schema(self, mock_get_spark, mock_get_dbs, mock_get_tables, mock_schema):
        """Test get_db_structure with schema."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_dbs.return_value = ["my.demo"]
        mock_get_tables.return_value = ["table1"]
        mock_schema.return_value = ["col1", "col2"]

        result = get_db_structure(with_schema=True, return_json=False)

        assert result == {"my.demo": {"table1": ["col1", "col2"]}}

    @patch("berdl_notebook_utils.spark.data_store.get_tables")
    @patch("berdl_notebook_utils.spark.data_store.get_databases")
    @patch("berdl_notebook_utils.spark.data_store._get_spark")
    def test_get_db_structure_returns_json(self, mock_get_spark, mock_get_dbs, mock_get_tables):
        """Test get_db_structure returns JSON string."""
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark
        mock_get_dbs.return_value = ["my.demo"]
        mock_get_tables.return_value = ["t1"]

        result = get_db_structure(with_schema=False, return_json=True)

        assert json.loads(result) == {"my.demo": ["t1"]}
