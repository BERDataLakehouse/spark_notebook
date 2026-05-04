"""
Unit tests for client creation functions.
"""

from unittest.mock import patch

from cdmtaskserviceclient.client import CTSClient

from berdl_notebook_utils import get_task_service_client
from berdl_notebook_utils.clients import get_minio_client, get_s3_client


def test_get_cts_client() -> None:
    """Test the get_task_service_client function. Disable the connection test."""
    with patch.object(CTSClient, "_test_cts_connection") as mock_test_connection:
        a = get_task_service_client()
        mock_test_connection.assert_called_once()
        assert isinstance(a, CTSClient)


def test_get_minio_client_is_alias_for_get_s3_client() -> None:
    """The legacy `get_minio_client` name must remain bound to `get_s3_client`.

    `data_lakehouse_ingest` <= v0.0.8 (and any other downstream we
    haven't audited) does ``from berdl_notebook_utils.clients import
    get_minio_client`` at module top-level. If that import ImportErrors,
    the IPython notebook startup chain in 00-notebookutils.py silently
    aborts and every subsequent startup script is skipped (see
    https://github.com/BERDataLakehouse/spark_notebook for context).

    This test ensures the backward-compat alias stays in place; remove
    it only when every downstream consumer has migrated to
    `get_s3_client`.
    """
    assert get_minio_client is get_s3_client
