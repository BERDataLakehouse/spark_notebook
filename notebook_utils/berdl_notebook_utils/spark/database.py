"""
Spark database utilities for BERDL notebook environments.

This module contains utility functions to interact with the Spark catalog,
including tenant-aware namespace management for BERDL SQL warehouses.
"""

from pyspark.sql import SparkSession
from berdl_notebook_utils.minio_governance.operations import (
    get_namespace_prefix,
    get_my_sql_warehouse,
    get_group_sql_warehouse,
)

DEFAULT_NAMESPACE = "default"


def _namespace_norm(namespace: str | None = DEFAULT_NAMESPACE) -> str:
    """Strips whitespace from the supplied namespace; returns the default namespace is nothing is supplied."""
    if not namespace:
        return DEFAULT_NAMESPACE
    ns = namespace.strip()
    return ns or DEFAULT_NAMESPACE


def generate_namespace_location(
    namespace: str | None = DEFAULT_NAMESPACE, tenant_name: str | None = None
) -> tuple[str, str | None]:
    """Generate the appropriate user or tenant warehouse namespace and its proposed location.

    Note that this function does not check for existing namespaces.

    :param namespace: input namespace, defaults to "default"
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
        print(
            f"Warning: Could not determine target name from warehouse directory '{warehouse_dir}'. "
            f"Using namespace as-is."
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
            namespace, db_location = generate_namespace_location(namespace, tenant_name)
        except Exception as e:
            print(f"Error creating namespace: {e}")
            raise e
    else:
        namespace = _namespace_norm(namespace)

    if spark.catalog.databaseExists(namespace):
        print(f"Namespace {namespace} is already registered and ready to use")
        return namespace

    # Create database with explicit LOCATION for Spark Connect compatibility
    if db_location:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace} LOCATION '{db_location}'")
        print(f"Namespace {namespace} is ready to use at location {db_location}.")
    else:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {namespace}")
        print(f"Namespace {namespace} is ready to use.")

    return namespace


def table_exists(
    spark: SparkSession,
    table_name: str,
    namespace: str = DEFAULT_NAMESPACE,
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
    db_table = f"{namespace}.{table_name}"

    exists = spark.catalog.tableExists(db_table)
    print(f"Table {db_table} {'exists' if exists else 'does not exist'}.")
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
    spark_catalog = f"{namespace}.{table_name}"

    spark.sql(f"DROP TABLE IF EXISTS {spark_catalog}")
    print(f"Table {spark_catalog} removed.")


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
        tables = [row["tableName"] for row in tables_df.collect()]
        return tables
    except Exception as e:
        print(f"Error listing tables: {e}")
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
        namespaces = [row["namespace"] for row in namespaces_df.collect()]
        return namespaces
    except Exception as e:
        print(f"Error listing namespaces: {e}")
        return []


def get_table_info(spark: SparkSession, table_name: str, namespace: str = DEFAULT_NAMESPACE) -> dict:
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
        >>> print(f"Table location: {info.get('location', 'N/A')}")
    """
    spark_catalog = f"{namespace}.{table_name}"

    try:
        # Get table description
        desc_df = spark.sql(f"DESCRIBE EXTENDED {spark_catalog}")

        # Convert to dictionary
        info = {}
        for row in desc_df.collect():
            if row["col_name"] and row["data_type"]:
                info[row["col_name"]] = row["data_type"]

        return info
    except Exception as e:
        print(f"Error getting table info for {spark_catalog}: {e}")
        return {}
