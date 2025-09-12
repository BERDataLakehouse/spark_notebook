"""
Unit tests for client creation functions.
"""

import os
from unittest.mock import patch

from cdmtaskserviceclient.client import CTSClient

from berdl_notebook_utils import get_task_service_client

env_vars = {
    "KBASE_AUTH_TOKEN": "test-token-123",
    "CDM_TASK_SERVICE_URL": "https://ci.kbase.us/services/ctsfake",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_SECURE_FLAG": "false",
    "BERDL_POD_IP": "192.168.1.100",
    "SPARK_MASTER_URL": "spark://localhost:7077",
    "SPARK_JOB_LOG_DIR_CATEGORY": "test-user",
    "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
    "SPARK_CLUSTER_MANAGER_API_URL": "http://localhost:8000",
    "GOVERNANCE_API_URL": "http://localhost:8000",
}


def test_get_cts_client():
    """Test the get_task_service_client function. Disable the connection test."""
    for key, value in env_vars.items():
        os.environ[key] = value

    with patch.object(CTSClient, "_test_cts_connection") as mock_test_connection:
        a = get_task_service_client()
        mock_test_connection.assert_called_once()
        assert type(a) is CTSClient
