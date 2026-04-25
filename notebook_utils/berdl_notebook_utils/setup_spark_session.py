"""
Spark utilities for CDM JupyterHub.

This module provides utilities for creating and configuring Spark sessions
with support for Delta Lake, MinIO S3 storage, and fair scheduling.

# This file must be loaded AFTER the 02-get_minio_client.py file
"""

import re
import warnings
from datetime import datetime
from typing import Any

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from .berdl_settings import BERDLSettings, get_settings
from .minio_governance.operations import (
    get_group_sql_warehouse,
    get_my_sql_warehouse,
)

# Suppress Protobuf version warnings from PySpark Spark Connect
warnings.filterwarnings("ignore", category=UserWarning, module="google.protobuf.runtime_version")

# Suppress CANNOT_MODIFY_CONFIG warnings for Hive metastore settings in Spark Connect
warnings.filterwarnings("ignore", category=UserWarning, module="pyspark.sql.connect.conf")

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


def convert_memory_format(memory_str: str, overhead_percentage: float = 0.1) -> str:
    """
    Convert memory format from profile format to Spark format with overhead adjustment.

    Args:
        memory_str: Memory string in profile format (supports B, KiB, MiB, GiB, TiB)
        overhead_percentage: Percentage of memory to reserve for system overhead (default: 0.1 = 10%)

    Returns:
        Memory string in Spark format with overhead accounted for
    """
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


def _get_executor_conf(settings: BERDLSettings, use_spark_connect: bool) -> dict[str, str]:
    """
    Get Spark executor and driver configuration based on profile settings.

    Args:
        settings: BERDLSettings instance with profile-specific configuration
        use_spark_connect: bool indicating whether or not spark connect is to be used

    Returns:
        Dictionary of Spark executor and driver configuration
    """
    # Convert memory formats from profile to Spark format with overhead adjustment
    executor_memory = convert_memory_format(settings.SPARK_WORKER_MEMORY, EXECUTOR_MEMORY_OVERHEAD)
    driver_memory = convert_memory_format(settings.SPARK_MASTER_MEMORY, DRIVER_MEMORY_OVERHEAD)

    if use_spark_connect:
        # Include KBase auth token for Spark Connect authentication
        base_url = str(settings.SPARK_CONNECT_URL).rstrip("/")
        spark_connect_url = f"{base_url}/;x-kbase-token={settings.KBASE_AUTH_TOKEN}"
        conf_base = {"spark.remote": spark_connect_url}
    else:
        # Legacy mode: add driver/executor configs
        conf_base = {
            "spark.driver.host": settings.BERDL_POD_IP,
            "spark.master": str(settings.SPARK_MASTER_URL),
        }

    return {
        **conf_base,
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


def _get_spark_defaults_conf() -> dict[str, str]:
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
    }


def _sanitize_catalog_alias(value: str) -> str:
    """Normalize a value into a Spark/Trino-compatible catalog alias."""
    return re.sub(r"[^a-z0-9_]", "_", value.lower()).strip("_")


def _get_personal_catalog_aliases(personal_catalog: str | None) -> list[str]:
    """Return Spark aliases for the current user's personal Polaris catalog."""
    if not personal_catalog:
        return []

    aliases = ["my"]
    portable_alias = personal_catalog.strip()
    if portable_alias.startswith("user_"):
        portable_alias = portable_alias[len("user_") :]
    portable_alias = _sanitize_catalog_alias(portable_alias)
    if portable_alias and portable_alias not in aliases:
        aliases.append(portable_alias)
    return aliases


def _get_tenant_catalog_alias(tenant_catalog: str) -> str:
    """Return the short engine alias for a Polaris tenant catalog."""
    alias = tenant_catalog.strip()
    if alias.startswith("tenant_"):
        alias = alias[len("tenant_") :]
    return _sanitize_catalog_alias(alias)


def _get_catalog_conf(settings: BERDLSettings) -> dict[str, str]:
    """Get Iceberg catalog configuration for Polaris REST catalog."""
    config = {}

    if not settings.POLARIS_CATALOG_URI:
        return config

    polaris_uri = str(settings.POLARIS_CATALOG_URI).rstrip("/")

    # S3/MinIO properties for Iceberg's S3FileIO (used by executors to read/write data files).
    # Iceberg does NOT use Spark's spark.hadoop.fs.s3a.* — it has its own AWS SDK S3 client.
    s3_endpoint = settings.MINIO_ENDPOINT_URL
    if not s3_endpoint.startswith("http"):
        s3_endpoint = f"http://{s3_endpoint}"
    s3_props = {
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": settings.MINIO_ACCESS_KEY,
        "s3.secret-access-key": settings.MINIO_SECRET_KEY,
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }

    def _catalog_props(prefix: str, warehouse: str) -> dict[str, str]:
        props = {
            f"{prefix}": "org.apache.iceberg.spark.SparkCatalog",
            f"{prefix}.type": "rest",
            f"{prefix}.uri": polaris_uri,
            f"{prefix}.credential": settings.POLARIS_CREDENTIAL or "",
            f"{prefix}.warehouse": warehouse,
            f"{prefix}.scope": "PRINCIPAL_ROLE:ALL",
            f"{prefix}.token-refresh-enabled": "false",
            f"{prefix}.client.region": "us-east-1",
        }
        # Add S3 properties scoped to this catalog
        for k, v in s3_props.items():
            props[f"{prefix}.{k}"] = v
        return props

    # 1. Add Personal Catalog aliases (if configured)
    if settings.POLARIS_PERSONAL_CATALOG:
        for catalog_alias in _get_personal_catalog_aliases(settings.POLARIS_PERSONAL_CATALOG):
            config.update(_catalog_props(f"spark.sql.catalog.{catalog_alias}", settings.POLARIS_PERSONAL_CATALOG))

    # 2. Add Tenant Catalogs (if configured)
    if settings.POLARIS_TENANT_CATALOGS:
        for tenant_catalog in settings.POLARIS_TENANT_CATALOGS.split(","):
            tenant_catalog = tenant_catalog.strip()
            if not tenant_catalog:
                continue
            catalog_alias = _get_tenant_catalog_alias(tenant_catalog)
            if not catalog_alias:
                continue
            config.update(_catalog_props(f"spark.sql.catalog.{catalog_alias}", tenant_catalog))

    return config


def _get_delta_conf() -> dict[str, str]:
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        # Delta Lake optimizations
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
    }


def _get_hive_conf(settings: BERDLSettings) -> dict[str, str]:
    # Do not set spark.sql.hive.metastore.version / .jars — forcing version=4.0.0
    # selects Shim_v4_0, which calls a 4-arg Hive.alterTable overload that only
    # exists in Hive 4.x clients. The bundled jars are Hive 2.3.10. See
    # configs/spark-defaults.conf.template for the full rationale.
    return {
        "spark.hadoop.hive.metastore.uris": str(settings.BERDL_HIVE_METASTORE_URI),
        "spark.sql.catalogImplementation": "hive",
    }


def _get_s3_conf(settings: BERDLSettings, tenant_name: str | None = None) -> dict[str, str]:
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
    # Use tenant SQL warehouse if a tenant name is supplied; otherwise, use the user's warehouse.
    warehouse_response = get_group_sql_warehouse(tenant_name) if tenant_name else get_my_sql_warehouse()

    event_log_dir = f"s3a://cdm-spark-job-logs/spark-job-logs/{settings.USER}/"

    return {
        "spark.hadoop.fs.s3a.endpoint": settings.MINIO_ENDPOINT_URL,
        "spark.hadoop.fs.s3a.access.key": settings.MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": settings.MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(settings.MINIO_SECURE).lower(),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.warehouse.dir": warehouse_response.sql_warehouse_prefix,
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_log_dir,
    }


IMMUTABLE_CONFIGS = {
    # Cluster-level settings (must be set at master startup)
    "spark.decommission.enabled",
    "spark.storage.decommission.rddBlocks.enabled",
    "spark.reducer.maxSizeInFlight",
    "spark.shuffle.file.buffer",
    # Driver and executor resource configs (locked at server startup)
    "spark.driver.memory",
    "spark.driver.cores",
    "spark.executor.instances",
    "spark.executor.cores",
    "spark.executor.memory",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.shuffleTracking.enabled",
    # Event logging (locked at server startup)
    "spark.eventLog.enabled",
    "spark.eventLog.dir",
    # SQL extensions (must be loaded at startup)
    "spark.sql.extensions",
    "spark.sql.catalog.spark_catalog",
    # Hive catalog (locked at startup)
    "spark.sql.catalogImplementation",
    # Warehouse directory (locked at server startup)
    "spark.sql.warehouse.dir",
}


def _is_immutable_config(key: str) -> bool:
    """Check if a Spark config key is immutable in Spark Connect mode."""
    if key in IMMUTABLE_CONFIGS:
        return True
    # Iceberg catalog configs (spark.sql.catalog.<name>.*) are all static/immutable
    # because catalogs must be registered at server startup. The catalog names are
    # dynamic (personal "my" + tenant aliases), so we match by prefix.
    if key.startswith("spark.sql.catalog.") and key != "spark.sql.catalog.spark_catalog":
        return True
    return False


def _filter_immutable_spark_connect_configs(config: dict[str, str]) -> dict[str, str]:
    """
    Filter out configurations that cannot be modified in Spark Connect mode.

    These configs must be set server-side when the Spark Connect server starts.
    Attempting to set them from the client results in CANNOT_MODIFY_CONFIG warnings.

    Args:
        config: Dictionary of Spark configurations

    Returns:
        Filtered configuration dictionary with only mutable configs

    """
    return {k: v for k, v in config.items() if not _is_immutable_config(k)}


def _set_scheduler_pool(spark: SparkSession, scheduler_pool: str) -> None:
    """Set the scheduler pool for the Spark session."""
    if scheduler_pool not in SPARK_POOLS:
        print(
            f"Warning: Scheduler pool '{scheduler_pool}' not in available pools: {SPARK_POOLS}. "
            f"Defaulting to '{SPARK_DEFAULT_POOL}'"
        )
        scheduler_pool = SPARK_DEFAULT_POOL

    spark.sparkContext.setLocalProperty("spark.scheduler.pool", scheduler_pool)


def generate_spark_conf(
    app_name: str | None = None,
    local: bool = False,
    use_delta_lake: bool = True,
    use_s3: bool = True,
    use_hive: bool = True,
    settings: BERDLSettings | None = None,
    tenant_name: str | None = None,
    use_spark_connect: bool = True,
) -> dict[str, str]:
    """Generate a spark session configuration dictionary from a set of input variables."""
    # Generate app name if not provided
    if app_name is None:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        app_name = f"kbase_spark_session_{timestamp}"

    # Build common configuration dictionary
    config: dict[str, str] = {"spark.app.name": app_name}

    if use_delta_lake:
        config.update(_get_delta_conf())

    if not local:
        # Add default Spark configurations
        config.update(_get_spark_defaults_conf())

        if settings is None:
            get_settings.cache_clear()
            settings = get_settings()

        # Add profile-specific executor and driver configuration
        config.update(_get_executor_conf(settings, use_spark_connect))

        if use_s3:
            config.update(_get_s3_conf(settings, tenant_name))

        if use_hive:
            config.update(_get_hive_conf(settings))

        # Always add Polaris catalogs if they are configured
        config.update(_get_catalog_conf(settings))

        if use_spark_connect:
            # Spark Connect: filter out immutable configs that cannot be modified from the client
            config = _filter_immutable_spark_connect_configs(config)

    return config


# =============================================================================
# PUBLIC FUNCTIONS
# =============================================================================


def get_spark_session(
    app_name: str | None = None,
    local: bool = False,
    # TODO: switch to `use_delta_lake` for consistency with s3 / hive
    delta_lake: bool = True,
    scheduler_pool: str = SPARK_DEFAULT_POOL,
    use_s3: bool = True,
    use_hive: bool = True,
    settings: BERDLSettings | None = None,
    tenant_name: str | None = None,
    use_spark_connect: bool = True,
    override: dict[str, Any] | None = None,
) -> SparkSession:
    """
    Create and configure a Spark session with BERDL-specific settings.

    This function creates a Spark session configured for the BERDL environment,
    including support for Delta Lake, MinIO S3 storage, and tenant-aware warehouses.

    Args:
        app_name: Application name. If None, generates a timestamp-based name
        local: If True, creates a local Spark session; the only other allowable option is `delta_lake`
        delta_lake: If True, enables Delta Lake support with required JARs
        scheduler_pool: Fair scheduler pool name (default: "default")
        use_s3: if True, enables reading from and writing to s3
        use_hive: If True, enables Hive metastore integration
        settings: BERDLSettings instance. If None, creates new instance from env vars
        tenant_name: Tenant/group name to use for SQL warehouse location. If specified,
                     tables will be written to the tenant's SQL warehouse instead
                     of the user's personal warehouse.
        use_spark_connect: If True, uses Spark Connect instead of legacy mode
        override: dictionary of tag-value pairs to replace the values in the generated spark conf (e.g. for testing)

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
    config = generate_spark_conf(
        app_name, local, delta_lake, use_s3, use_hive, settings, tenant_name, use_spark_connect
    )
    if override:
        config.update(override)

    spark_conf = SparkConf().setAll(list(config.items()))
    spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    # Post-creation configuration (only for legacy mode with SparkContext)
    if not local and not use_spark_connect:
        spark.sparkContext.setLogLevel("DEBUG")
        _set_scheduler_pool(spark, scheduler_pool)

    # Warm up Polaris REST catalogs so they appear in SHOW CATALOGS immediately.
    # Spark lazily initializes REST catalog plugins — they only show up in
    # CatalogManager._catalogs (and therefore SHOW CATALOGS) after first access.
    if use_spark_connect and not local:
        _settings = settings or get_settings()
        _catalog_aliases: list[str] = []
        _catalog_aliases.extend(_get_personal_catalog_aliases(_settings.POLARIS_PERSONAL_CATALOG))
        if _settings.POLARIS_TENANT_CATALOGS:
            for _raw in _settings.POLARIS_TENANT_CATALOGS.split(","):
                _raw = _raw.strip()
                if _raw:
                    _alias = _get_tenant_catalog_alias(_raw)
                    if _alias:
                        _catalog_aliases.append(_alias)
        for _alias in _catalog_aliases:
            try:
                spark.sql(f"SHOW NAMESPACES IN {_alias}").collect()
            except Exception:
                pass  # catalog may not have any namespaces yet; access is enough to register it

    return spark
