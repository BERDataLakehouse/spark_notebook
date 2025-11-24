"""
Unit tests for client creation functions.
"""

from unittest.mock import patch

from cdmtaskserviceclient.client import CTSClient

from berdl_notebook_utils import get_task_service_client


def test_get_cts_client() -> None:
    """Test the get_task_service_client function. Disable the connection test."""
    with patch.object(CTSClient, "_test_cts_connection") as mock_test_connection:
        a = get_task_service_client()
        mock_test_connection.assert_called_once()
        assert isinstance(a, CTSClient)
