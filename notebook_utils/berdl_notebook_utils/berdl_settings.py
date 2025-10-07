"""
Simple environment validation using Pydantic Settings.
"""

import logging
from functools import lru_cache

from pydantic import AnyHttpUrl, AnyUrl, Field, ValidationError
from pydantic_settings import BaseSettings

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
    MINIO_ACCESS_KEY: str = Field(default="access_key_placeholder", description="MinIO access key")
    MINIO_SECRET_KEY: str = Field(default="secret_key_placeholder", description="MinIO secret key")
    MINIO_SECURE: bool = Field(default=False, description="Use secure connection (True/False)")

    # Spark configuration
    BERDL_POD_IP: str
    SPARK_HOME: str = Field(default="/usr/local/spark", description="Spark installation directory")
    SPARK_MASTER_URL: AnyUrl  # Accepts spark://, http://, https://
    SPARK_CONNECT_URL: AnyUrl = Field(
        default=AnyUrl("sc://localhost:15002"), description="Spark Connect URL (sc://host:port)"
    )
    SPARK_CONNECT_DEFAULTS_TEMPLATE: str = Field(
        default="/configs/spark-defaults.conf.template",
        description="Path to Spark Connect server configuration template file",
    )

    # Hive configuration
    BERDL_HIVE_METASTORE_URI: AnyUrl  # Accepts thrift://

    # Profile-specific Spark configuration from JupyterHub
    SPARK_WORKER_COUNT: int = Field(default=1, description="Number of Spark workers from profile")
    SPARK_WORKER_CORES: int = Field(default=1, description="Cores per Spark worker from profile")
    SPARK_WORKER_MEMORY: str = Field(
        default="2GiB",
        pattern=r"^\d+[kmgKMGT]i?[bB]?$",
        description="Memory per Spark worker from profile",
    )
    SPARK_MASTER_CORES: int = Field(default=1, description="Cores for Spark master from profile")
    SPARK_MASTER_MEMORY: str = Field(
        default="1GiB",
        pattern=r"^\d+[kmgKMGT]i?[bB]?$",
        description="Memory for Spark master from profile",
    )

    # Spark Cluster Manager API configuration
    SPARK_CLUSTER_MANAGER_API_URL: AnyHttpUrl

    # Data Governance API configuration
    GOVERNANCE_API_URL: AnyHttpUrl


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
