"""
Simple unit tests for environment validation.
"""

from berdl_notebook_utils import validate_environment

env_vars = {
    "KBASE_AUTH_TOKEN": "test-token-123",
    "CDM_TASK_SERVICE_URL": "http://localhost:8080",
    "MINIO_ENDPOINT": "http://localhost:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "MINIO_SECURE_FLAG": "false",
    "BERDL_POD_IP": "192.168.1.100",
    "SPARK_MASTER_URL": "spark://localhost:7077",
    "SPARK_JOB_LOG_DIR_CATEGORY": "test-user",
    "BERDL_HIVE_METASTORE_URI": "thrift://localhost:9083",
}


def test_valid_environment(monkeypatch):
    """Test that all valid environment variables return empty list."""
    # Set all required environment variables

    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    result = validate_environment()
    assert result == []


def test_missing_variables(monkeypatch):
    """Test that missing variables are returned in list."""
    # Only set some variables
    env_vars_with_missing = env_vars.copy()
    env_vars_with_missing["BERDL_HIVE_METASTORE_URI"] = ""
    for key, value in env_vars_with_missing.items():
        monkeypatch.setenv(key, value)

    result = validate_environment()

    assert isinstance(result, list)
    assert len(result) > 0
    print(result)
