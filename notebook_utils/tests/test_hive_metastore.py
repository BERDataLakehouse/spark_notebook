"""
Tests for hive_metastore.py module.
"""

from unittest.mock import Mock, patch
import pytest

from berdl_notebook_utils.hive_metastore import get_databases, get_tables


class TestGetDatabases:
    """Tests for get_databases function."""

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_databases_returns_list(self, mock_get_client):
        """Test get_databases returns list of database names."""
        mock_client = Mock()
        mock_client.get_databases.return_value = ["db1", "db2", "db3"]
        mock_get_client.return_value = mock_client

        result = get_databases()

        assert result == ["db1", "db2", "db3"]
        mock_client.open.assert_called_once()
        mock_client.get_databases.assert_called_once_with("*")
        mock_client.close.assert_called_once()

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_databases_empty_list(self, mock_get_client):
        """Test get_databases returns empty list when no databases."""
        mock_client = Mock()
        mock_client.get_databases.return_value = []
        mock_get_client.return_value = mock_client

        result = get_databases()

        assert result == []

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_databases_closes_on_error(self, mock_get_client):
        """Test get_databases closes client on error."""
        mock_client = Mock()
        mock_client.get_databases.side_effect = Exception("Connection error")
        mock_get_client.return_value = mock_client

        with pytest.raises(Exception, match="Connection error"):
            get_databases()

        mock_client.close.assert_called_once()


class TestGetTables:
    """Tests for get_tables function."""

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_tables_returns_list(self, mock_get_client):
        """Test get_tables returns list of table names."""
        mock_client = Mock()
        mock_client.get_tables.return_value = ["table1", "table2"]
        mock_get_client.return_value = mock_client

        result = get_tables("test_db")

        assert result == ["table1", "table2"]
        mock_client.open.assert_called_once()
        mock_client.get_tables.assert_called_once_with("test_db", "*")
        mock_client.close.assert_called_once()

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_tables_empty_database(self, mock_get_client):
        """Test get_tables returns empty list for empty database."""
        mock_client = Mock()
        mock_client.get_tables.return_value = []
        mock_get_client.return_value = mock_client

        result = get_tables("empty_db")

        assert result == []

    @patch("berdl_notebook_utils.hive_metastore.get_hive_metastore_client")
    def test_get_tables_closes_on_error(self, mock_get_client):
        """Test get_tables closes client on error."""
        mock_client = Mock()
        mock_client.get_tables.side_effect = Exception("Database not found")
        mock_get_client.return_value = mock_client

        with pytest.raises(Exception, match="Database not found"):
            get_tables("nonexistent_db")

        mock_client.close.assert_called_once()
