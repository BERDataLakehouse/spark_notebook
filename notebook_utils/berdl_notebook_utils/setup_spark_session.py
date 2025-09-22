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

from berdl_notebook_utils import BERDLSettings, get_settings
from berdl_notebook_utils.minio_governance import (
    get_my_sql_warehouse,
    get_group_sql_warehouse,
)

# =============================================================================
# CONSTANTS
# =============================================================================

# Fair scheduler configuration
SPARK_DEFAULT_POOL = "default"
SPARK_POOLS = [SPARK_DEFAULT_POOL, "highPriority"]

# Memory overhead percentages for Spark components
EXECUTOR_MEMORY_OVERHEAD = 0.1  # 10% overhead for executors (accounts for JVM + system overhead)
DRIVER_MEMORY_OVERHEAD = 0.05  # 5% overhead for driver (typically less memory pressure)

# =============================================================================
# PRIVATE HELPER FUNCTIONS
# =============================================================================


def _convert_memory_format(memory_str: str, overhead_percentage: float = 0.1) -> str:
    """
    Convert memory format from profile format to Spark format with overhead adjustment.

    Args:
        memory_str: Memory string in profile format (supports B, KiB, MiB, GiB, TiB)
        overhead_percentage: Percentage of memory to reserve for system overhead (default: 0.1 = 10%)

    Returns:
        Memory string in Spark format with overhead accounted for
    """
    import re

    # Extract number and unit from memory string
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([kmgtKMGT]i?[bB]?)$", memory_str)
    if not match:
        raise ValueError(f"Invalid memory format: {memory_str}")

    value, unit = match.groups()
    value = float(value)

    # Convert to bytes for calculation
    unit_lower = unit.lower()
    multipliers = {
        "b": 1,
        "kb": 1024,
        "kib": 1024,
        "mb": 1024**2,
        "mib": 1024**2,
        "gb": 1024**3,
        "gib": 1024**3,
        "tb": 1024**4,
        "tib": 1024**4,
    }

    # Remove trailing 'b' if present for lookup
    unit_key = unit_lower.rstrip("b") + "b" if unit_lower.endswith("b") else unit_lower + "b"
    if unit_key not in multipliers:
        unit_key = unit_lower

    bytes_value = value * multipliers.get(unit_key, multipliers["b"])

    # Apply overhead reduction (reserve percentage for system)
    adjusted_bytes = bytes_value * (1 - overhead_percentage)

    # Convert back to appropriate Spark unit (prefer GiB for larger values)
    if adjusted_bytes >= 1024**3:
        adjusted_value = adjusted_bytes / (1024**3)
        spark_unit = "g"
    elif adjusted_bytes >= 1024**2:
        adjusted_value = adjusted_bytes / (1024**2)
        spark_unit = "m"
    elif adjusted_bytes >= 1024:
        adjusted_value = adjusted_bytes / 1024
        spark_unit = "k"
    else:
        adjusted_value = adjusted_bytes
        spark_unit = ""

    # Format as integer to ensure Spark compatibility
    # Some Spark versions don't accept fractional memory values
    return f"{int(round(adjusted_value))}{spark_unit}"


def _get_executor_config(settings: BERDLSettings) -> Dict[str, str]:
    """
    Get Spark executor and driver configuration based on profile settings.

    Args:
        settings: BERDLSettings instance with profile-specific configuration

    Returns:
        Dictionary of Spark executor and driver configuration
    """
    # Convert memory formats from profile to Spark format with overhead adjustment
    executor_memory = _convert_memory_format(settings.SPARK_WORKER_MEMORY, EXECUTOR_MEMORY_OVERHEAD)
    driver_memory = _convert_memory_format(settings.SPARK_MASTER_MEMORY, DRIVER_MEMORY_OVERHEAD)

    config = {
        # Driver configuration (critical for remote cluster connections)
        "spark.driver.memory": driver_memory,
        "spark.driver.cores": str(settings.SPARK_MASTER_CORES),
        # Executor configuration
        "spark.executor.instances": str(settings.SPARK_WORKER_COUNT),
        "spark.executor.cores": str(settings.SPARK_WORKER_CORES),
        "spark.executor.memory": executor_memory,
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


def _get_s3_conf(settings: BERDLSettings, tenant_name: Optional[str] = None) -> Dict[str, str]:
    """
    Get S3 configuration for MinIO.

    Args:
        settings: BERDLSettings instance with configuration
        tenant_name: Tenant/group name to use for SQL warehouse. If provided,
                    configures Spark to write tables to the tenant's SQL warehouse.
                    If None, uses the user's personal SQL warehouse.

    Returns:
        Dictionary of S3/MinIO Spark configuration properties
    """

    if tenant_name:
        # Use tenant's SQL warehouse
        tenant_warehouse_response = get_group_sql_warehouse(tenant_name)
        warehouse_dir = tenant_warehouse_response.sql_warehouse_prefix
    else:
        # Use user's personal SQL warehouse
        user_warehouse_response = get_my_sql_warehouse()
        warehouse_dir = user_warehouse_response.sql_warehouse_prefix

    event_log_dir = f"s3a://cdm-spark-job-logs/spark-job-logs/{settings.USER}/"

    config = {
        "spark.hadoop.fs.s3a.endpoint": settings.MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": settings.MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": settings.MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
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
    tenant_name: Optional[str] = None,
) -> SparkSession:
    """
    Create and configure a Spark session with BERDL-specific settings.

    This function creates a Spark session configured for the BERDL environment,
    including support for Delta Lake, MinIO S3 storage, and tenant-aware warehouses.

    Args:
        app_name: Application name. If None, generates a timestamp-based name
        local: If True, creates a local Spark session (ignores other configs)
        delta_lake: If True, enables Delta Lake support with required JARs
        scheduler_pool: Fair scheduler pool name (default: "default")
        use_hive: If True, enables Hive metastore integration
        settings: BERDLSettings instance. If None, creates new instance from env vars
        tenant_name: Tenant/group name to use for SQL warehouse location. If specified,
                     tables will be written to the tenant's SQL warehouse instead
                     of the user's personal warehouse.

    Returns:
        Configured SparkSession instance

    Raises:
        EnvironmentError: If required environment variables are missing
        ValueError: If user is not a member of the specified tenant

    Example:
        >>> # Basic usage (user's personal warehouse)
        >>> spark = get_spark_session("MyApp")

        >>> # Using tenant warehouse (writes to tenant's SQL directory)
        >>> spark = get_spark_session("MyApp", tenant_name="research_team")

        >>> # With custom scheduler pool
        >>> spark = get_spark_session("MyApp", scheduler_pool="highPriority")

        >>> # Local development
        >>> spark = get_spark_session("TestApp", local=True)
    """

    if settings is None:
        settings = get_settings()

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
        config.update(_get_s3_conf(settings, tenant_name))
    if use_hive:
        config["hive.metastore.uris"] = str(settings.BERDL_HIVE_METASTORE_URI)
        config["spark.sql.catalogImplementation"] = "hive"
        config["spark.sql.hive.metastore.version"] = "4.0.0"
        config["spark.sql.hive.metastore.jars"] = "path"
        config["spark.sql.hive.metastore.jars.path"] = "/usr/local/spark/jars/*"

    # Create and configure Spark session
    spark_conf = SparkConf().setAll(list(config.items()))
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")

    # Set scheduler pool
    _set_scheduler_pool(spark, scheduler_pool)

    return spark
