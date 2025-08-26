"""
Spark utilities for CDM JupyterHub.

This module provides utilities for creating and configuring Spark sessions
with support for Delta Lake, MinIO S3 storage, and fair scheduling.

# This file must be loaded AFTER the 02-get_minio_client.py file
"""

from datetime import datetime
from typing import Dict, Optional

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from berdl_notebook_utils import BERDLSettings

# =============================================================================
# CONSTANTS
# =============================================================================

# Spark executor defaults and get them from settings.. We will need to set them in settings though
settings = BERDLSettings()
DEFAULT_EXECUTOR_CORES = 1
DEFAULT_EXECUTOR_MEMORY = "2g"
DEFAULT_MAX_EXECUTORS = 5

# Fair scheduler configuration
SPARK_DEFAULT_POOL = "default"
SPARK_POOLS = [SPARK_DEFAULT_POOL, "highPriority"]

# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================


def _get_s3_conf(settings: BERDLSettings) -> Dict[str, str]:
    """
    Get S3 configuration for MinIO.
    """
    warehouse_dir = f"s3a://cdm-lake/users-sql-warehouse/{settings.USER}/"

    config = {
        "spark.hadoop.fs.s3a.endpoint": settings.MINIO_ENDPOINT_URL,
        "spark.hadoop.fs.s3a.access.key": settings.MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": settings.MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.warehouse.dir": warehouse_dir,
    }

    config.update(
        {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        }
    )
    return config


def _set_scheduler_pool(spark: SparkSession, scheduler_pool: str) -> None:
    """Set the scheduler pool for the Spark session."""
    if scheduler_pool not in SPARK_POOLS:
        print(
            f"Warning: Scheduler pool '{scheduler_pool}' not in available pools: {SPARK_POOLS}. "
            f"Defaulting to '{SPARK_DEFAULT_POOL}'"
        )
        scheduler_pool = SPARK_DEFAULT_POOL

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", scheduler_pool)


# =============================================================================
# PUBLIC FUNCTIONS
# =============================================================================


def get_spark_session(
    app_name: Optional[str] = None,
    local: bool = False,
    delta_lake: bool = True,
    scheduler_pool: str = SPARK_DEFAULT_POOL,
    use_hive: bool = True,
    settings: BERDLSettings = None,
) -> SparkSession:
    """
    Create and configure a Spark session with CDM-specific settings.

    This function creates a Spark session configured for the CDM environment,
    including support for Delta Lake, MinIO S3 storage

    Args:
        app_name: Application name. If None, generates a timestamp-based name
        local: If True, creates a local Spark session (ignores other configs)
        delta_lake: If True, enables Delta Lake support with required JARs
        scheduler_pool: Fair scheduler pool name (default: "default")

    Returns:
        Configured SparkSession instance

    Raises:
        EnvironmentError: If required environment variables are missing
        FileNotFoundError: If required JAR files are missing

    Example:
        >>> # Basic usage
        >>> spark = get_spark_session("MyApp")

        >>> # With custom scheduler pool
        >>> spark = get_spark_session("MyApp", scheduler_pool="highPriority")

        >>> # Local development
        >>> spark = get_spark_session("TestApp", local=True)
    """
    # TODO TODO since we disabled dynamic allocation, we need to set the executor cores and memory here
    # TODO: Ensure hub passes the settings of the cores and memory of the executors, so the client can set them here,
    # TODO: Set Spark Driver Settings too

    if settings is None:
        settings = BERDLSettings()

    # Generate app name if not provided
    if app_name is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        app_name = f"kbase_spark_session_{timestamp}"

    # For local development, return simple session
    if local:
        return SparkSession.builder.appName(app_name).getOrCreate()

    # Build configuration dictionary
    config: Dict[str, str] = {
        "spark.app.name": app_name,
        "spark.driver.host": settings.BERDL_POD_IP,
        "spark.master": str(settings.SPARK_MASTER_URL),
    }

    # Configure driver host
    if delta_lake:
        config.update(_get_s3_conf(settings))
    if use_hive:
        config["hive.metastore.uris"] = str(settings.BERDL_HIVE_METASTORE_URI)

    # Create and configure Spark session
    spark_conf = SparkConf().setAll(list(config.items()))
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    # Set scheduler pool
    _set_scheduler_pool(spark, scheduler_pool)

    return spark
