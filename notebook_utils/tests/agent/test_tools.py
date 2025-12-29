"""
Tests for agent/tools.py - LangChain tools for BERDL MCP operations.
"""

from unittest.mock import Mock, patch
import pytest


class TestFormatResult:
    """Tests for _format_result helper function."""

    def test_format_list(self):
        """Test formatting list result."""
        from berdl_notebook_utils.agent.tools import _format_result

        result = _format_result(["db1", "db2", "db3"])

        assert '"db1"' in result
        assert '"db2"' in result
        assert '"db3"' in result

    def test_format_dict(self):
        """Test formatting dict result."""
        from berdl_notebook_utils.agent.tools import _format_result

        result = _format_result({"key": "value"})

        assert '"key"' in result
        assert '"value"' in result

    def test_format_string(self):
        """Test formatting string result."""
        from berdl_notebook_utils.agent.tools import _format_result

        result = _format_result("simple string")

        assert result == "simple string"


class TestListDatabases:
    """Tests for list_databases tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_databases_success(self, mock_mcp_ops):
        """Test list_databases returns formatted databases."""
        from berdl_notebook_utils.agent.tools import list_databases

        mock_mcp_ops.mcp_list_databases.return_value = ["db1", "db2"]

        result = list_databases()

        assert "db1" in result
        assert "db2" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_databases_empty(self, mock_mcp_ops):
        """Test list_databases returns message when empty."""
        from berdl_notebook_utils.agent.tools import list_databases

        mock_mcp_ops.mcp_list_databases.return_value = []

        result = list_databases()

        assert "No databases found" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_databases_error(self, mock_mcp_ops):
        """Test list_databases handles errors."""
        from berdl_notebook_utils.agent.tools import list_databases

        mock_mcp_ops.mcp_list_databases.side_effect = Exception("Connection failed")

        result = list_databases()

        assert "Error listing databases" in result


class TestListTables:
    """Tests for list_tables tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_tables_success(self, mock_mcp_ops):
        """Test list_tables returns formatted tables."""
        from berdl_notebook_utils.agent.tools import list_tables

        mock_mcp_ops.mcp_list_tables.return_value = ["table1", "table2"]

        result = list_tables("test_db")

        assert "table1" in result
        assert "table2" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_tables_empty(self, mock_mcp_ops):
        """Test list_tables returns message when empty."""
        from berdl_notebook_utils.agent.tools import list_tables

        mock_mcp_ops.mcp_list_tables.return_value = []

        result = list_tables("empty_db")

        assert "No tables found" in result
        assert "empty_db" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_list_tables_error(self, mock_mcp_ops):
        """Test list_tables handles errors."""
        from berdl_notebook_utils.agent.tools import list_tables

        mock_mcp_ops.mcp_list_tables.side_effect = Exception("Database not found")

        result = list_tables("nonexistent")

        assert "Error listing tables" in result


class TestGetTableSchema:
    """Tests for get_table_schema tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_get_table_schema_success(self, mock_mcp_ops):
        """Test get_table_schema returns formatted schema."""
        from berdl_notebook_utils.agent.tools import get_table_schema

        mock_mcp_ops.mcp_get_table_schema.return_value = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
        ]

        result = get_table_schema("test_db", "test_table")

        assert "id" in result
        assert "name" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_get_table_schema_error(self, mock_mcp_ops):
        """Test get_table_schema handles errors."""
        from berdl_notebook_utils.agent.tools import get_table_schema

        mock_mcp_ops.mcp_get_table_schema.side_effect = Exception("Table not found")

        result = get_table_schema("db", "table")

        assert "Error getting table schema" in result


class TestGetDatabaseStructure:
    """Tests for get_database_structure tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_get_database_structure_without_schema(self, mock_mcp_ops):
        """Test get_database_structure without schema."""
        from berdl_notebook_utils.agent.tools import get_database_structure

        mock_mcp_ops.mcp_get_database_structure.return_value = {
            "db1": ["table1", "table2"]
        }

        result = get_database_structure("false")

        assert "db1" in result
        assert "table1" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_get_database_structure_with_schema(self, mock_mcp_ops):
        """Test get_database_structure with schema."""
        from berdl_notebook_utils.agent.tools import get_database_structure

        mock_mcp_ops.mcp_get_database_structure.return_value = {
            "db1": {"table1": ["col1", "col2"]}
        }

        result = get_database_structure("true")

        assert "db1" in result
        assert "table1" in result
        assert "col1" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_get_database_structure_error(self, mock_mcp_ops):
        """Test get_database_structure handles errors."""
        from berdl_notebook_utils.agent.tools import get_database_structure

        mock_mcp_ops.mcp_get_database_structure.side_effect = Exception("Error")

        result = get_database_structure()

        assert "Error getting database structure" in result


class TestSampleTable:
    """Tests for sample_table tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_sample_table_success(self, mock_mcp_ops):
        """Test sample_table returns formatted data."""
        from berdl_notebook_utils.agent.tools import sample_table

        mock_mcp_ops.mcp_sample_table.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        result = sample_table("test_db", "test_table")

        assert "Alice" in result
        assert "Bob" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_sample_table_with_params(self, mock_mcp_ops):
        """Test sample_table with limit, columns, and where clause."""
        from berdl_notebook_utils.agent.tools import sample_table

        mock_mcp_ops.mcp_sample_table.return_value = [{"id": 1}]

        result = sample_table(
            database="db",
            table="tbl",
            limit=5,
            columns=["id"],
            where_clause="id > 0",
        )

        mock_mcp_ops.mcp_sample_table.assert_called_once_with(
            database="db",
            table="tbl",
            limit=5,
            columns=["id"],
            where_clause="id > 0",
        )

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_sample_table_error(self, mock_mcp_ops):
        """Test sample_table handles errors."""
        from berdl_notebook_utils.agent.tools import sample_table

        mock_mcp_ops.mcp_sample_table.side_effect = Exception("Error")

        result = sample_table("db", "table")

        assert "Error sampling table" in result


class TestCountTableRows:
    """Tests for count_table_rows tool function."""

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_count_table_rows_success(self, mock_mcp_ops):
        """Test count_table_rows returns formatted count."""
        from berdl_notebook_utils.agent.tools import count_table_rows

        mock_mcp_ops.mcp_count_table.return_value = 12345

        result = count_table_rows("test_db", "test_table")

        assert "12,345" in result
        assert "test_db.test_table" in result

    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_count_table_rows_error(self, mock_mcp_ops):
        """Test count_table_rows handles errors."""
        from berdl_notebook_utils.agent.tools import count_table_rows

        mock_mcp_ops.mcp_count_table.side_effect = Exception("Error")

        result = count_table_rows("db", "table")

        assert "Error counting rows" in result


class TestQueryTable:
    """Tests for query_table tool function."""

    @patch("berdl_notebook_utils.agent.tools.get_agent_settings")
    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_query_table_success(self, mock_mcp_ops, mock_settings):
        """Test query_table returns formatted results."""
        from berdl_notebook_utils.agent.tools import query_table

        mock_settings.return_value.AGENT_SQL_ROW_LIMIT = 1000
        mock_mcp_ops.mcp_query_table.return_value = [{"col": "val"}]

        result = query_table("SELECT * FROM db.table LIMIT 10")

        assert "col" in result
        assert "val" in result

    @patch("berdl_notebook_utils.agent.tools.get_agent_settings")
    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_query_table_empty_query(self, mock_mcp_ops, mock_settings):
        """Test query_table rejects empty query."""
        from berdl_notebook_utils.agent.tools import query_table

        mock_settings.return_value.AGENT_SQL_ROW_LIMIT = 1000

        result = query_table("   ")

        assert "Error: Query cannot be empty" in result

    @patch("berdl_notebook_utils.agent.tools.get_agent_settings")
    def test_query_table_no_limit_warning(self, mock_settings):
        """Test query_table warns when no LIMIT clause."""
        from berdl_notebook_utils.agent.tools import query_table

        mock_settings.return_value.AGENT_SQL_ROW_LIMIT = 100

        result = query_table("SELECT * FROM db.table")

        assert "Warning" in result
        assert "LIMIT" in result

    @patch("berdl_notebook_utils.agent.tools.get_agent_settings")
    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_query_table_truncates_large_results(self, mock_mcp_ops, mock_settings):
        """Test query_table truncates results over 100 rows."""
        from berdl_notebook_utils.agent.tools import query_table

        mock_settings.return_value.AGENT_SQL_ROW_LIMIT = 1000
        mock_mcp_ops.mcp_query_table.return_value = [{"id": i} for i in range(150)]

        result = query_table("SELECT * FROM db.table LIMIT 150")

        assert "Showing first 100 of 150" in result

    @patch("berdl_notebook_utils.agent.tools.get_agent_settings")
    @patch("berdl_notebook_utils.agent.tools.mcp_ops")
    def test_query_table_error(self, mock_mcp_ops, mock_settings):
        """Test query_table handles errors."""
        from berdl_notebook_utils.agent.tools import query_table

        mock_settings.return_value.AGENT_SQL_ROW_LIMIT = 1000
        mock_mcp_ops.mcp_query_table.side_effect = Exception("SQL error")

        result = query_table("SELECT * FROM db.table LIMIT 10")

        assert "Error executing query" in result


class TestToolDefinitions:
    """Tests for tool definition functions."""

    def test_get_discovery_tools(self):
        """Test get_discovery_tools returns correct tools."""
        from berdl_notebook_utils.agent.tools import get_discovery_tools

        tools = get_discovery_tools()

        assert len(tools) == 4
        tool_names = [t.name for t in tools]
        assert "list_databases" in tool_names
        assert "list_tables" in tool_names
        assert "get_table_schema" in tool_names
        assert "get_database_structure" in tool_names

    def test_get_data_inspection_tools(self):
        """Test get_data_inspection_tools returns correct tools."""
        from berdl_notebook_utils.agent.tools import get_data_inspection_tools

        tools = get_data_inspection_tools()

        assert len(tools) == 2
        tool_names = [t.name for t in tools]
        assert "sample_table" in tool_names
        assert "count_table_rows" in tool_names

    def test_get_query_tools(self):
        """Test get_query_tools returns correct tools."""
        from berdl_notebook_utils.agent.tools import get_query_tools

        tools = get_query_tools()

        assert len(tools) == 1
        assert tools[0].name == "query_table"

    def test_get_all_tools_with_sql(self):
        """Test get_all_tools includes SQL tools when enabled."""
        from berdl_notebook_utils.agent.tools import get_all_tools

        tools = get_all_tools(enable_sql_execution=True)

        tool_names = [t.name for t in tools]
        assert "query_table" in tool_names
        assert len(tools) == 7

    def test_get_all_tools_without_sql(self):
        """Test get_all_tools excludes SQL tools when disabled."""
        from berdl_notebook_utils.agent.tools import get_all_tools

        tools = get_all_tools(enable_sql_execution=False)

        tool_names = [t.name for t in tools]
        assert "query_table" not in tool_names
        assert len(tools) == 6
