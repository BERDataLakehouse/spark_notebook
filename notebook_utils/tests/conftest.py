"""Shared test helpers and bootstrap for notebook_utils tests."""

import os


TEST_ENVIRONMENT = {
    "USER": "fake_user",
    "KBASE_AUTH_TOKEN": "test-token-123",
    "CDM_TASK_SERVICE_URL": "http://localhost:8080",
    "MINIO_ENDPOINT_URL": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_SECURE": "false",
    "BERDL_POD_IP": "192.168.1.100",
    "SPARK_MASTER_URL": "spark://localhost:7077",
    "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
    "SPARK_CLUSTER_MANAGER_API_URL": "http://localhost:8000",
    "GOVERNANCE_API_URL": "http://localhost:8000",
    "DATALAKE_MCP_SERVER_URL": "http://localhost:8080",
}


for key, value in TEST_ENVIRONMENT.items():
    os.environ.setdefault(key, value)


class WarehouseResponse:
    """Fake Governance service response to getting the user or group warehouse prefix."""

    def __init__(self, value: str) -> None:
        self.sql_warehouse_prefix = value
