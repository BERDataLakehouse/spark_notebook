"""
Tests for mcp/operations.py - MCP wrapper functions.
"""

from unittest.mock import Mock, patch
import pytest

from berdl_notebook_utils.mcp.operations import (
    _handle_error_response,
    mcp_list_databases,
    mcp_list_tables,
    mcp_get_table_schema,
    mcp_get_database_structure,
    mcp_count_table,
    mcp_sample_table,
    mcp_query_table,
    mcp_select_table,
)


@pytest.fixture(autouse=True)
def clear_client_cache():
    """Clear client cache before each test."""
    from berdl_notebook_utils.mcp.client import get_datalake_mcp_client

    get_datalake_mcp_client.cache_clear()
    yield


@pytest.fixture
def mock_client():
    """Create a mock MCP client."""
    with patch("berdl_notebook_utils.mcp.operations.get_datalake_mcp_client") as mock_get_client:
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        yield mock_client


class TestHandleErrorResponse:
    """Tests for _handle_error_response helper function."""

    def test_does_nothing_for_non_error_response(self):
        """Test that non-error responses are ignored."""
        response = Mock()
        response.__class__.__name__ = "SomeResponse"

        # Should not raise
        _handle_error_response(response, "test_operation")

    def test_raises_for_error_response(self):
        """Test that ErrorResponse raises an exception."""
        from datalake_mcp_server_client.models import ErrorResponse

        error_response = ErrorResponse(error="Something went wrong")

        with pytest.raises(Exception) as exc_info:
            _handle_error_response(error_response, "test_operation")

        assert "MCP Server error" in str(exc_info.value)
        assert "test_operation" in str(exc_info.value)
        assert "Something went wrong" in str(exc_info.value)


class TestMcpListDatabases:
    """Tests for mcp_list_databases function."""

    def test_returns_list_of_databases(self, mock_client):
        """Test that mcp_list_databases returns a list of database names."""
        mock_response = Mock()
        mock_response.databases = ["default", "analytics", "user_data"]

        with patch("berdl_notebook_utils.mcp.operations.list_databases") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_list_databases()

            assert result == ["default", "analytics", "user_data"]

    def test_passes_use_hms_parameter(self, mock_client):
        """Test that use_hms parameter is passed correctly."""
        mock_response = Mock()
        mock_response.databases = []

        with patch("berdl_notebook_utils.mcp.operations.list_databases") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_list_databases(use_hms=False)

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].use_hms is False

    def test_raises_on_none_response(self, mock_client):
        """Test that None response raises an exception."""
        with patch("berdl_notebook_utils.mcp.operations.list_databases") as mock_api:
            mock_api.sync.return_value = None

            with pytest.raises(Exception) as exc_info:
                mcp_list_databases()

            assert "no response" in str(exc_info.value).lower()


class TestMcpListTables:
    """Tests for mcp_list_tables function."""

    def test_returns_list_of_tables(self, mock_client):
        """Test that mcp_list_tables returns a list of table names."""
        mock_response = Mock()
        mock_response.tables = ["users", "orders", "products"]

        with patch("berdl_notebook_utils.mcp.operations.list_database_tables") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_list_tables("my_database")

            assert result == ["users", "orders", "products"]

    def test_passes_database_parameter(self, mock_client):
        """Test that database parameter is passed correctly."""
        mock_response = Mock()
        mock_response.tables = []

        with patch("berdl_notebook_utils.mcp.operations.list_database_tables") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_list_tables("analytics")

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].database == "analytics"


class TestMcpGetTableSchema:
    """Tests for mcp_get_table_schema function."""

    def test_returns_column_list(self, mock_client):
        """Test that mcp_get_table_schema returns column names."""
        mock_response = Mock()
        mock_response.columns = ["id", "name", "email", "created_at"]

        with patch("berdl_notebook_utils.mcp.operations.get_table_schema") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_get_table_schema("my_db", "users")

            assert result == ["id", "name", "email", "created_at"]

    def test_passes_database_and_table(self, mock_client):
        """Test that database and table parameters are passed correctly."""
        mock_response = Mock()
        mock_response.columns = []

        with patch("berdl_notebook_utils.mcp.operations.get_table_schema") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_get_table_schema("analytics", "events")

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].database == "analytics"
            assert call_kwargs["body"].table == "events"


class TestMcpGetDatabaseStructure:
    """Tests for mcp_get_database_structure function."""

    def test_returns_structure_dict(self, mock_client):
        """Test that mcp_get_database_structure returns structure dictionary."""
        mock_response = Mock()
        mock_response.structure = {"default": ["table1", "table2"], "analytics": ["events"]}

        with patch("berdl_notebook_utils.mcp.operations.get_database_structure") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_get_database_structure()

            assert result == {"default": ["table1", "table2"], "analytics": ["events"]}

    def test_with_schema_parameter(self, mock_client):
        """Test that with_schema parameter is passed correctly."""
        mock_response = Mock()
        mock_response.structure = {}

        with patch("berdl_notebook_utils.mcp.operations.get_database_structure") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_get_database_structure(with_schema=True)

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].with_schema is True


class TestMcpCountTable:
    """Tests for mcp_count_table function."""

    def test_returns_row_count(self, mock_client):
        """Test that mcp_count_table returns the row count."""
        mock_response = Mock()
        mock_response.count = 1000000

        with patch("berdl_notebook_utils.mcp.operations.count_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_count_table("my_db", "users")

            assert result == 1000000

    def test_passes_database_and_table(self, mock_client):
        """Test that database and table parameters are passed correctly."""
        mock_response = Mock()
        mock_response.count = 0

        with patch("berdl_notebook_utils.mcp.operations.count_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_count_table("analytics", "events")

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].database == "analytics"
            assert call_kwargs["body"].table == "events"


class TestMcpSampleTable:
    """Tests for mcp_sample_table function."""

    def test_returns_sample_rows(self, mock_client):
        """Test that mcp_sample_table returns sample data."""
        mock_response = Mock()
        mock_response.sample = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        with patch("berdl_notebook_utils.mcp.operations.sample_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_sample_table("my_db", "users")

            assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_passes_limit_parameter(self, mock_client):
        """Test that limit parameter is passed correctly."""
        mock_response = Mock()
        mock_response.sample = []

        with patch("berdl_notebook_utils.mcp.operations.sample_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_sample_table("db", "table", limit=5)

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].limit == 5

    def test_passes_columns_parameter(self, mock_client):
        """Test that columns parameter is passed correctly."""
        mock_response = Mock()
        mock_response.sample = []

        with patch("berdl_notebook_utils.mcp.operations.sample_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_sample_table("db", "table", columns=["id", "name"])

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].columns == ["id", "name"]

    def test_passes_where_clause(self, mock_client):
        """Test that where_clause parameter is passed correctly."""
        mock_response = Mock()
        mock_response.sample = []

        with patch("berdl_notebook_utils.mcp.operations.sample_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_sample_table("db", "table", where_clause="status = 'active'")

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].where_clause == "status = 'active'"


class TestMcpQueryTable:
    """Tests for mcp_query_table function."""

    def test_returns_query_results(self, mock_client):
        """Test that mcp_query_table returns query results."""
        mock_response = Mock()
        mock_response.result = [
            {"username": "alice", "count": 42},
            {"username": "bob", "count": 38},
        ]

        with patch("berdl_notebook_utils.mcp.operations.query_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_query_table("SELECT * FROM users LIMIT 10")

            assert result == [{"username": "alice", "count": 42}, {"username": "bob", "count": 38}]

    def test_passes_query(self, mock_client):
        """Test that the query is passed correctly."""
        mock_response = Mock()
        mock_response.result = []

        with patch("berdl_notebook_utils.mcp.operations.query_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_query_table("SELECT COUNT(*) FROM events")

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].query == "SELECT COUNT(*) FROM events"


class TestMcpSelectTable:
    """Tests for mcp_select_table function."""

    def test_returns_data_and_pagination(self, mock_client):
        """Test that mcp_select_table returns data and pagination info."""
        mock_item1 = Mock()
        mock_item1.to_dict.return_value = {"id": 1, "name": "Alice"}
        mock_item2 = Mock()
        mock_item2.to_dict.return_value = {"id": 2, "name": "Bob"}

        mock_pagination = Mock()
        mock_pagination.limit = 100
        mock_pagination.offset = 0
        mock_pagination.total_count = 2
        mock_pagination.has_more = False

        mock_response = Mock()
        mock_response.data = [mock_item1, mock_item2]
        mock_response.pagination = mock_pagination

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            result = mcp_select_table("my_db", "users")

            assert result["data"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
            assert result["pagination"]["limit"] == 100
            assert result["pagination"]["offset"] == 0
            assert result["pagination"]["total_count"] == 2
            assert result["pagination"]["has_more"] is False

    def test_with_string_columns(self, mock_client):
        """Test that string columns are handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table("db", "table", columns=["id", "name"])

            call_kwargs = mock_api.sync.call_args[1]
            columns_spec = call_kwargs["body"].columns
            assert len(columns_spec) == 2
            assert columns_spec[0].column == "id"
            assert columns_spec[1].column == "name"

    def test_with_dict_columns(self, mock_client):
        """Test that dict columns with aliases are handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table(
                "db",
                "table",
                columns=[{"column": "user_id", "alias": "id", "table_alias": "u"}],
            )

            call_kwargs = mock_api.sync.call_args[1]
            columns_spec = call_kwargs["body"].columns
            assert len(columns_spec) == 1
            assert columns_spec[0].column == "user_id"
            assert columns_spec[0].alias == "id"
            assert columns_spec[0].table_alias == "u"

    def test_with_filters(self, mock_client):
        """Test that filters are handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table(
                "db",
                "table",
                filters=[{"column": "status", "operator": "=", "value": "active"}],
            )

            call_kwargs = mock_api.sync.call_args[1]
            filters_spec = call_kwargs["body"].filters
            assert len(filters_spec) == 1
            assert filters_spec[0].column == "status"
            assert filters_spec[0].value == "active"

    def test_with_aggregations(self, mock_client):
        """Test that aggregations are handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table(
                "db",
                "table",
                aggregations=[{"function": "COUNT", "column": "*", "alias": "total"}],
            )

            call_kwargs = mock_api.sync.call_args[1]
            agg_spec = call_kwargs["body"].aggregations
            assert len(agg_spec) == 1
            assert agg_spec[0].column == "*"
            assert agg_spec[0].alias == "total"

    def test_with_joins(self, mock_client):
        """Test that joins are handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table(
                "db",
                "orders",
                joins=[
                    {
                        "join_type": "LEFT",
                        "database": "db",
                        "table": "users",
                        "on_left_column": "user_id",
                        "on_right_column": "id",
                    }
                ],
            )

            call_kwargs = mock_api.sync.call_args[1]
            joins_spec = call_kwargs["body"].joins
            assert len(joins_spec) == 1
            assert joins_spec[0].table == "users"

    def test_with_order_by_string(self, mock_client):
        """Test that string order_by is handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table("db", "table", order_by=["name", "created_at"])

            call_kwargs = mock_api.sync.call_args[1]
            order_spec = call_kwargs["body"].order_by
            assert len(order_spec) == 2
            assert order_spec[0].column == "name"
            assert order_spec[1].column == "created_at"

    def test_with_order_by_dict(self, mock_client):
        """Test that dict order_by with direction is handled correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table(
                "db",
                "table",
                order_by=[{"column": "created_at", "direction": "DESC"}],
            )

            call_kwargs = mock_api.sync.call_args[1]
            order_spec = call_kwargs["body"].order_by
            assert len(order_spec) == 1
            assert order_spec[0].column == "created_at"

    def test_with_pagination_params(self, mock_client):
        """Test that limit and offset parameters are passed correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=10, offset=20, total_count=100, has_more=True)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table("db", "table", limit=10, offset=20)

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].limit == 10
            assert call_kwargs["body"].offset == 20

    def test_with_distinct(self, mock_client):
        """Test that distinct parameter is passed correctly."""
        mock_response = Mock()
        mock_response.data = []
        mock_response.pagination = Mock(limit=100, offset=0, total_count=0, has_more=False)

        with patch("berdl_notebook_utils.mcp.operations.select_delta_table") as mock_api:
            mock_api.sync.return_value = mock_response

            mcp_select_table("db", "table", distinct=True)

            call_kwargs = mock_api.sync.call_args[1]
            assert call_kwargs["body"].distinct is True
