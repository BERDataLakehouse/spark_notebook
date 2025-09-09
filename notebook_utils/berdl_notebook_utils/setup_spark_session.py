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

# Fair scheduler configuration
SPARK_DEFAULT_POOL = "default"
SPARK_POOLS = [SPARK_DEFAULT_POOL, "highPriority"]

# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================


def _convert_memory_format(memory_str: str) -> str:
    """
    Convert memory format from profile format (e.g., "2GiB", "1024MiB") to Spark format (e.g., "2g", "1024m").

    Args:
        memory_str: Memory string in profile format (supports B, KiB, MiB, GiB, TiB)

    Returns:
        Memory string in Spark format (uses k, m, g, t)
    """
    # Handle case-insensitive conversion
    memory_lower = memory_str.lower()

    # Remove 'i' from binary units (KiB -> KB, MiB -> MB, etc.)
    if "ib" in memory_lower:
        memory_lower = memory_lower.replace("ib", "")
    elif memory_lower.endswith("b"):
        memory_lower = memory_lower[:-1]  # Remove trailing 'b'

    # Convert to Spark format (lowercase without 'B')
    return memory_lower


def _get_executor_config(settings: BERDLSettings) -> Dict[str, str]:
    """
    Get Spark executor configuration based on profile settings.

    Args:
        settings: BERDLSettings instance with profile-specific configuration

    Returns:
        Dictionary of Spark executor configuration
    """
    # Convert memory formats from profile to Spark format
    executor_memory = _convert_memory_format(settings.DEFAULT_WORKER_MEMORY)
    driver_memory = _convert_memory_format(settings.DEFAULT_MASTER_MEMORY)

    config = {
        # Executor configuration
        "spark.executor.instances": str(settings.DEFAULT_WORKER_COUNT),
        "spark.executor.cores": str(settings.DEFAULT_WORKER_CORES),
        "spark.executor.memory": executor_memory,
        # Driver configuration
        "spark.driver.cores": str(settings.DEFAULT_MASTER_CORES),
        "spark.driver.memory": driver_memory,
        # Disable dynamic allocation since we're setting explicit instances
        "spark.dynamicAllocation.enabled": "false",
        "spark.dynamicAllocation.shuffleTracking.enabled": "false",
    }

    return config


def _get_spark_defaults_conf() -> Dict[str, str]:
    """
    Get Spark defaults configuration.
    """

    return {
        # Decommissioning
        "spark.decommission.enabled": "true",
        "spark.storage.decommission.rddBlocks.enabled": "true",
        # Broadcast join configurations
        "spark.sql.autoBroadcastJoinThreshold": "52428800",  # 50MB (default is 10MB)
        # Shuffle and compression configurations
        "spark.reducer.maxSizeInFlight": "96m",  # 96MB (default is 48MB)
        "spark.shuffle.file.buffer": "1m",  # 1MB (default is 32KB)
        # Delta Lake optimizations
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
    }


def _get_s3_conf(settings: BERDLSettings) -> Dict[str, str]:
    """
    Get S3 configuration for MinIO.
    """
    warehouse_dir = f"s3a://cdm-lake/users-sql-warehouse/{settings.USER}/"
    event_log_dir = f"s3a://cdm-spark-job-logs/spark-job-logs/{settings.USER}/"

    config = {
        "spark.hadoop.fs.s3a.endpoint": settings.MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": settings.MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": settings.MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.warehouse.dir": warehouse_dir,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_log_dir,
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    }

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
    settings: Optional[BERDLSettings] = None,
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

    Parameters
    ----------
    """
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

    # Add default Spark configurations
    config.update(_get_spark_defaults_conf())

    # Add profile-specific executor and driver configuration
    config.update(_get_executor_config(settings))

    # Configure driver host
    if delta_lake:
        config.update(_get_s3_conf(settings))
    if use_hive:
        config["hive.metastore.uris"] = str(settings.BERDL_HIVE_METASTORE_URI)
        config["spark.sql.catalogImplementation"] = "hive"

    # Create and configure Spark session
    spark_conf = SparkConf().setAll(list(config.items()))
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    # Set scheduler pool
    _set_scheduler_pool(spark, scheduler_pool)

    return spark
