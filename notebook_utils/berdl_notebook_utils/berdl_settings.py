"""
Simple environment validation using Pydantic Settings.
"""

import logging
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import ValidationError, AnyUrl, Field, AnyHttpUrl

# Configure logging
logger = logging.getLogger(__name__)


class BERDLSettings(BaseSettings):
    """
    BERDL environment configuration using Pydantic Settings.
    """

    # Core authentication
    KBASE_AUTH_TOKEN: str
    CDM_TASK_SERVICE_URL: AnyHttpUrl  # Accepts http:// and https://
    USER: str  # KBase username of the user running the notebook

    # MinIO configuration
    MINIO_ENDPOINT_URL: str = Field(..., description="MinIO endpoint (hostname:port)")
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_SECURE_FLAG: bool

    # Spark configuration
    BERDL_POD_IP: str
    SPARK_MASTER_URL: AnyUrl  # Accepts spark://, http://, https://
    SPARK_JOB_LOG_DIR_CATEGORY: str

    # Hive configuration
    BERDL_HIVE_METASTORE_URI: AnyUrl  # Accepts thrift://

    # Optional Spark settings
    MAX_EXECUTORS: int = 5
    EXECUTOR_CORES: int = 1
    EXECUTOR_MEMORY: str = Field(default="2g", pattern=r"^\d+[kmg]$", description="Spark executor memory (e.g., 2g, 1024m)")


def validate_environment():
    """
    Validate all required environment variables.

    Returns:
        List of missing/invalid environment variable names. Empty list if all are valid.
    """
    try:
        BERDLSettings()
        return []
    except ValidationError as e:
        return [error["loc"][0] for error in e.errors()]


@lru_cache(maxsize=1)
def get_settings() -> BERDLSettings:
    """
    Get cached BERDLSettings instance. Only creates the object once.

    Returns:
        BERDLSettings: Cached settings instance

    Raises:
        ValidationError: If environment variables are missing or invalid
    """
    return BERDLSettings()


# Auto-validate on import
def _auto_validate():
    """Automatically validate environment on module import."""
    missing_vars = validate_environment()
    if missing_vars:
        logger.error(f"❌ Missing or invalid environment variables: {missing_vars}")
        logger.error("BERDLHub Config Error! Please contact a BERDL system administrator.")


# Call validation when module is imported
_auto_validate()
