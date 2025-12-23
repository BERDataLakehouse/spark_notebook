"""
Spark database utilities for BERDL notebook environments.

This module contains utility functions to interact with the Spark catalog,
including tenant-aware namespace management for BERDL SQL warehouses.
"""

import logging

from pyspark.sql import SparkSession

from berdl_notebook_utils.minio_governance.operations import (
    get_group_sql_warehouse,
    get_my_sql_warehouse,
    get_namespace_prefix,
)

DEFAULT_NAMESPACE = "default"


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _namespace_norm(namespace: str | None = None) -> str:
    """Strips whitespace from the supplied namespace; returns the default namespace if nothing is supplied."""
    if not namespace:
        return DEFAULT_NAMESPACE
    ns = namespace.strip()
    return ns or DEFAULT_NAMESPACE


def generate_namespace_location(namespace: str | None = None, tenant_name: str | None = None) -> tuple[str, str | None]:
    """Generate the appropriate user or tenant warehouse namespace and its proposed location.

    Note that this function does not check for existing namespaces.

    :param namespace: input namespace, defaults to None
    :type namespace: str, optional
    :param tenant_name: name of the tenant; if absent, the namespace will be in the user warehouse. Defaults to None.
    :type tenant_name: str | None, optional
    :return: tuple of the namespace name and its location in the data warehouse
    :rtype: tuple[str, str|None]
    """
    namespace = _namespace_norm(namespace)
    db_location = None
    # Always fetch warehouse directory from governance API for proper S3 location
    # Don't rely on spark.sql.warehouse.dir as it may be set to local path by Spark Connect server
    warehouse_response = get_group_sql_warehouse(tenant_name) if tenant_name else get_my_sql_warehouse()
    warehouse_dir = warehouse_response.sql_warehouse_prefix

    if warehouse_dir and ("users-sql-warehouse" in warehouse_dir or "tenant-sql-warehouse" in warehouse_dir):
        # Extract target name (username or tenant name) from path
        # e.g. s3a://cdm-lake/users-sql-warehouse/tgu2
        # e.g. s3a://cdm-lake/tenant-sql-warehouse/global-user-group
        target_name = warehouse_dir.rstrip("/").split("/")[-1]

        # Get namespace prefix from governance client based on warehouse type
        if "users-sql-warehouse" in warehouse_dir:
            # User warehouse - get user namespace prefix
            prefix_response = get_namespace_prefix()
            namespace = prefix_response.user_namespace_prefix + namespace
        else:
            # Tenant warehouse - get tenant namespace prefix
            prefix_response = get_namespace_prefix(tenant=target_name)
            namespace = prefix_response.tenant_namespace_prefix + namespace

        # Set database location to warehouse_dir/namespace.db
        db_location = f"{warehouse_dir.rstrip('/')}/{namespace}.db"
    else:
        # Keep original namespace if warehouse path doesn't match expected patterns
        logger.warning(
            "Warning: Could not determine target name from warehouse directory '%s'. Using namespace as-is.",
            warehouse_dir,
        )

    return (namespace, db_location)


def create_namespace_if_not_exists(
    spark: SparkSession,
    namespace: str | None = DEFAULT_NAMESPACE,
    append_target: bool = True,
    tenant_name: str | None = None,
) -> str:
    """
    Create a namespace in the Spark catalog if it does not exist.

    If append_target is True, automatically prepends the governance-provided namespace prefix
    based on the warehouse directory type (user vs tenant) to create the properly formatted namespace.

    For Spark Connect, this function explicitly sets the database LOCATION to ensure tables are
    written to the correct S3 path, since spark.sql.warehouse.dir cannot be modified per session.

    :param spark: The Spark session.
    :param namespace: The name of the namespace.
    :param append_target: If True, prepends governance namespace prefix based on warehouse type.
                         If False, uses namespace as-is.
    :param tenant_name: Optional tenant name. If provided, uses tenant warehouse. Otherwise uses user warehouse.
    :return: The name of the namespace.
    """
    db_location = None

    if append_target:
        try:
            delta_namespace, db_location = generate_namespace_location(namespace, tenant_name)
        except Exception:
            # includes stack trace
            logger.exception("Error creating namespace")
            raise
    else:
        delta_namespace = _namespace_norm(namespace)

    if spark.catalog.databaseExists(delta_namespace):
        logger.info("Namespace %s is already registered and ready to use.", delta_namespace)
        return delta_namespace

    # Create database with explicit LOCATION for Spark Connect compatibility
    if db_location:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {delta_namespace} LOCATION '{db_location}'")
        logger.info("Namespace %s is ready to use at location %s.", delta_namespace, db_location)
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {delta_namespace}")
        logger.info("Namespace %s is ready to use.", delta_namespace)

    return delta_namespace


def table_exists(
    spark: SparkSession,
    table_name: str,
    namespace: str | None = None,
) -> bool:
    """
    Check if a table exists in the Spark catalog.

    Args:
        spark: The Spark session
        table_name: The name of the table
        namespace: The namespace of the table. Default is "default"

    Returns:
        True if the table exists, False otherwise

    Example:
        >>> if table_exists(spark, "user_data", "alice_experiments"):
        ...     print("Table exists")
    """
    if not namespace:
        namespace = DEFAULT_NAMESPACE

    db_table = f"{namespace}.{table_name}"
    exists = spark.catalog.tableExists(db_table)
    logger.info(f"Table {db_table} {'exists' if exists else 'does not exist'}.")
    return exists


def remove_table(
    spark: SparkSession,
    table_name: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> None:
    """
    Remove a table from the Spark catalog.

    Args:
        spark: The Spark session
        table_name: The name of the table
        namespace: The namespace of the table. Default is "default"

    Example:
        >>> remove_table(spark, "temp_data", "alice_experiments")
    """
    db_table = f"{namespace}.{table_name}"
    spark.sql(f"DROP TABLE IF EXISTS {db_table}")
    logger.info("Table %s removed.", db_table)


def list_tables(spark: SparkSession, namespace: str | None = None) -> list:
    """
    List all tables in a namespace or all namespaces.

    Args:
        spark: The Spark session
        namespace: The namespace to list tables from. If None, lists from current namespace

    Returns:
        List of table names

    Example:
        >>> tables = list_tables(spark, "alice_experiments")
        >>> print(f"Found {len(tables)} tables")
    """
    try:
        if namespace:
            tables_df = spark.sql(f"SHOW TABLES IN {namespace}")
        else:
            tables_df = spark.sql("SHOW TABLES")

        # Extract table names from the DataFrame
        return [row["tableName"] for row in tables_df.collect()]
    except Exception:
        logger.exception("Error listing tables")
        return []


def list_namespaces(spark: SparkSession) -> list:
    """
    List all namespaces (databases) in the Spark catalog.

    Args:
        spark: The Spark session

    Returns:
        List of namespace names

    Example:
        >>> namespaces = list_namespaces(spark)
        >>> print(f"Available namespaces: {namespaces}")
    """
    try:
        namespaces_df = spark.sql("SHOW DATABASES")
        return [row["namespace"] for row in namespaces_df.collect()]
    except Exception:
        logger.exception("Error listing namespaces")
        return []


def get_namespace_info(spark: SparkSession, namespace: str | None = None) -> dict:
    """
    Get detailed information about a namespace.

    Args:
        spark: The Spark session
        namespace: The namespace to retrieve information about

    Returns:
        Dictionary containing namespace information

    Example:
        >>> info = get_namespace_info(spark, "alice_experiments")
        >>> print(f"Namespace location: {info.get('Location', 'N/A')}")
    """
    if not namespace:
        namespace = DEFAULT_NAMESPACE

    info = {}
    try:
        # Get namespace description
        desc_df = spark.sql(f"DESCRIBE NAMESPACE EXTENDED {namespace}").collect()
        # Convert to dictionary
        info = {row["info_name"]: row["info_value"] for row in desc_df}
    except Exception:
        logger.exception("Error getting namespace info for %s", namespace)

    return info


def get_table_info(spark: SparkSession, table_name: str, namespace: str | None = None) -> dict:
    """
    Get detailed information about a table.

    Args:
        spark: The Spark session
        table_name: The name of the table
        namespace: The namespace of the table

    Returns:
        Dictionary containing table information

    Example:
        >>> info = get_table_info(spark, "user_data", "alice_experiments")
        >>> print(f"Table location: {info.get('Location', 'N/A')}")
    """
    if not namespace:
        namespace = DEFAULT_NAMESPACE
    db_table = f"{namespace}.{table_name}"
    info = {}
    try:
        # Get table description
        desc_df = spark.sql(f"DESCRIBE EXTENDED {db_table}").collect()
        # N.b. if the table contains columns with the same names as table metadata fields ("Name", "Type", "Location", "Provider", etc.)
        # they will be overwritten.
        info = {row["col_name"]: row["data_type"] for row in desc_df if row["col_name"] and row["data_type"]}
    except Exception:
        logger.exception("Error getting table info for %s", db_table)

    return info
