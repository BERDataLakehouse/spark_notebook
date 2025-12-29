"""Module for interacting with Spark databases and tables.

This module provides functions to retrieve information about databases, tables,
and their schemas from a Spark cluster or directly from Hive metastore in PostgreSQL.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from berdl_notebook_utils import hive_metastore
from pyspark.sql import SparkSession
from berdl_notebook_utils.setup_spark_session import get_spark_session
from berdl_notebook_utils.minio_governance import get_my_accessible_paths, get_my_groups, get_namespace_prefix

# =============================================================================
# TTL CACHE FOR GOVERNANCE API CALLS
# =============================================================================
# These caches prevent repeated slow API calls within a session.
# Default TTL is 5 minutes (300 seconds).

_CACHE_TTL_SECONDS = 300  # 5 minutes

T = TypeVar("T")


def _ttl_cache(ttl_seconds: int = _CACHE_TTL_SECONDS) -> Callable:
    """
    Simple TTL cache decorator using a dict-based cache.

    Unlike functools.lru_cache, this cache expires entries after ttl_seconds.
    Thread-safe for concurrent reads (Python's GIL protects dict operations).
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache: Dict[str, tuple[float, T]] = {}

        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            # Create cache key from function arguments
            key = str((args, tuple(sorted(kwargs.items()))))
            now = time.time()

            # Check if cached and not expired
            if key in cache:
                cached_time, cached_value = cache[key]
                if now - cached_time < ttl_seconds:
                    return cached_value

            # Call function and cache result
            result = func(*args, **kwargs)
            cache[key] = (now, result)
            return result

        def clear_cache() -> None:
            """Clear all cached entries."""
            cache.clear()

        wrapper.clear_cache = clear_cache  # type: ignore
        return wrapper

    return decorator


@_ttl_cache()
def _cached_get_my_groups():
    """Cached wrapper for get_my_groups()."""
    return get_my_groups()


@_ttl_cache()
def _cached_get_namespace_prefix(tenant: Optional[str] = None):
    """Cached wrapper for get_namespace_prefix()."""
    return get_namespace_prefix(tenant=tenant)


@_ttl_cache()
def _cached_get_my_accessible_paths():
    """Cached wrapper for get_my_accessible_paths()."""
    return get_my_accessible_paths()


def clear_governance_cache() -> None:
    """Clear all governance API caches. Call this when user permissions change."""
    getattr(_cached_get_my_groups, "clear_cache", lambda: None)()
    getattr(_cached_get_namespace_prefix, "clear_cache", lambda: None)()
    getattr(_cached_get_my_accessible_paths, "clear_cache", lambda: None)()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _execute_with_spark(func: Any, spark: Optional[SparkSession] = None, *args, **kwargs) -> Any:
    """
    Execute a function with a SparkSession, creating one if not provided.
    """
    if spark is None:
        spark = get_spark_session()
        return func(spark, *args, **kwargs)
    return func(spark, *args, **kwargs)


def _format_output(data: Any, return_json: bool = True) -> Union[str, Any]:
    """
    Format the output based on the return_json flag.
    """
    return json.dumps(data) if return_json else data


def _extract_databases_from_paths(paths: List[str]) -> List[str]:
    """
    Extract unique database names from S3 SQL warehouse paths.

    Only considers paths in SQL warehouses (not general warehouses or logs):
    - s3a://cdm-lake/users-sql-warehouse/...
    - s3a://cdm-lake/tenant-sql-warehouse/...

    S3 paths are in format: s3a://bucket/warehouse/user_or_tenant/database.db/...
    Example: s3a://cdm-lake/users-sql-warehouse/tgu1/u_tgu1__sharing_test.db/employee_records_1/

    Args:
        paths: List of S3 paths from accessible paths API

    Returns:
        List of unique database names (without .db suffix)
    """
    databases = set()
    for path in paths:
        # Only process paths from SQL warehouses
        if not any(warehouse in path for warehouse in ["/users-sql-warehouse/", "/tenant-sql-warehouse/"]):
            continue

        # Remove s3a:// prefix and split by /
        parts = path.replace("s3a://", "").split("/")

        # Look for .db directory (database directory in Hive convention)
        for part in parts:
            if part.endswith(".db"):
                db_name = part[:-3]  # Remove .db suffix
                databases.add(db_name)
                break

    return sorted(list(databases))


def get_databases(
    spark: Optional[SparkSession] = None,
    use_hms: bool = True,
    return_json: bool = True,
    filter_by_namespace: bool = True,
) -> Union[str, List[str]]:
    """
    Get the list of databases in the Hive metastore.

    Args:
        spark: Optional SparkSession to use (if use_hms is False)
        use_hms: Whether to use HMS direct client (faster) or Spark
        return_json: Whether to return JSON string or raw data
        filter_by_namespace: Whether to filter databases by user/group ownership AND shared access.
                           When True, returns:
                           - User's owned databases (u_username_*)
                           - Group/tenant databases (groupname_*)
                           - Databases shared with the user (from accessible paths API)
                           When False, returns all databases in the metastore.

    Returns:
        List of database names, either as JSON string or raw list
    """

    def _get_dbs(session: SparkSession) -> List[str]:
        return [db.name for db in session.catalog.listDatabases()]

    if use_hms:
        databases = hive_metastore.get_databases()
    else:
        databases = _execute_with_spark(_get_dbs, spark)

    # Apply filtering: owned databases (fast) + shared databases (API call)
    if filter_by_namespace:
        try:
            # Parallelize all governance API calls to reduce latency
            # These calls use cached wrappers, so subsequent calls are instant
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit initial calls: groups, user prefix, and accessible paths
                groups_future = executor.submit(_cached_get_my_groups)
                user_prefix_future = executor.submit(_cached_get_namespace_prefix)
                accessible_paths_future = executor.submit(_cached_get_my_accessible_paths)

                # Wait for groups to get tenant list, then submit tenant prefix calls
                groups_response = groups_future.result()
                tenant_prefix_futures = {
                    group: executor.submit(_cached_get_namespace_prefix, tenant=group)
                    for group in groups_response.groups
                }

                # Collect results
                user_prefix_response = user_prefix_future.result()
                prefixes = [user_prefix_response.user_namespace_prefix]

                # Collect tenant prefixes as they complete
                for group, future in tenant_prefix_futures.items():
                    tenant_prefix_response = future.result()
                    prefixes.append(tenant_prefix_response.tenant_namespace_prefix)

                # Filter databases by namespace prefixes (owned + group databases)
                owned_databases = [db for db in databases if db.startswith(tuple(prefixes))]

                # Get shared databases from accessible paths API
                accessible_paths_response = accessible_paths_future.result()
                shared_databases = _extract_databases_from_paths(accessible_paths_response.accessible_paths)

            # Combine owned and shared, remove duplicates
            all_accessible = set(owned_databases) | set(shared_databases)

            # Filter to only databases that exist in metastore, and sort for consistent output
            databases = sorted([db for db in databases if db in all_accessible])

        except Exception as e:
            raise Exception(f"Could not filter databases by namespace: {e}") from e

    return _format_output(databases, return_json)


def get_tables(
    database: str, spark: Optional[SparkSession] = None, use_hms: bool = True, return_json: bool = True
) -> Union[str, List[str]]:
    """
    Get the list of tables in a specific database.

    Args:
        database: Name of the database
        spark: Optional SparkSession to use (if use_hms is False)
        use_hms: Whether to use HMS direct client (faster) or Spark
        return_json: Whether to return JSON string or raw data

    Returns:
        List of table names, either as JSON string or raw list
    """

    def _get_tbls(session: SparkSession, db: str) -> List[str]:
        return [table.name for table in session.catalog.listTables(dbName=db)]

    if use_hms:
        tables = hive_metastore.get_tables(database)
    else:
        tables = _execute_with_spark(_get_tbls, spark, database)

    return _format_output(tables, return_json)


def get_table_schema(
    database: str, table: str, spark: Optional[SparkSession] = None, return_json: bool = True
) -> Union[str, List[str]]:
    """
    Get the schema of a specific table in a database.

    Args:
        database: Name of the database
        table: Name of the table
        spark: Optional SparkSession to use
        return_json: Whether to return JSON string or raw data

    Returns:
        List of column names, either as JSON string or raw list
    """

    def _get_schema(session: SparkSession, db: str, tbl: str) -> List[str]:
        try:
            return [column.name for column in session.catalog.listColumns(dbName=db, tableName=tbl)]
        except Exception:
            # Observed that certain tables lack their corresponding S3 files
            print(f"Error retrieving schema for table {tbl} in database {db}")
            return []

    columns = _execute_with_spark(_get_schema, spark, database, table)
    return _format_output(columns, return_json)


def get_db_structure(
    with_schema: bool = False,
    use_hms: bool = True,
    return_json: bool = True,
    filter_by_namespace: bool = True,
) -> Union[str, Dict]:
    """Get the structure of all databases in the Hive metastore.

    Args:
        with_schema: Whether to include table schemas
        use_hms: Whether to use HMS direct client for metadata retrieval
        return_json: Whether to return the result as a JSON string
        filter_by_namespace: Whether to filter databases by user/tenant namespace prefixes

    Returns:
        Database structure as either JSON string or dictionary:
        {
            "database_name": ["table1", "table2"] or
            "database_name": {
                "table1": ["column1", "column2"],
                "table2": ["column1", "column2"]
            }
        }
    """

    def _get_structure(session: SparkSession) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
        db_structure = {}
        databases = get_databases(
            spark=session, use_hms=False, return_json=False, filter_by_namespace=filter_by_namespace
        )

        for db in databases:
            tables = get_tables(database=db, spark=session, use_hms=False, return_json=False)
            if with_schema:
                db_structure[db] = {
                    table: get_table_schema(database=db, table=table, spark=session, return_json=False)
                    for table in tables
                }
            else:
                db_structure[db] = tables

        return db_structure

    if use_hms:
        db_structure = {}
        databases = get_databases(use_hms=True, return_json=False, filter_by_namespace=filter_by_namespace)

        for db in databases:
            tables = hive_metastore.get_tables(db)
            if with_schema:
                # Get schema using Spark session
                spark = get_spark_session()
                db_structure[db] = {
                    table: get_table_schema(database=db, table=table, spark=spark, return_json=False)
                    for table in tables
                }
            else:
                db_structure[db] = tables

    else:
        db_structure = _execute_with_spark(_get_structure)

    return _format_output(db_structure, return_json)
