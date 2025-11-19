"""Module for interacting with Spark databases and tables.

This module provides functions to retrieve information about databases, tables,
and their schemas from a Spark cluster or directly from Hive metastore in PostgreSQL.
"""

import json
from typing import Any, Dict, List, Optional, Union

from .. import hive_metastore
from pyspark.sql import SparkSession
from ..setup_spark_session import get_spark_session
from ..minio_governance import get_my_groups, get_namespace_prefix


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
        filter_by_namespace: Whether to filter databases by user/tenant namespace prefixes

    Returns:
        List of database names, either as JSON string or raw list
    """

    def _get_dbs(session: SparkSession) -> List[str]:
        return [db.name for db in session.catalog.listDatabases()]

    if use_hms:
        databases = hive_metastore.get_databases()
    else:
        databases = _execute_with_spark(_get_dbs, spark)

    # Apply namespace filtering if requested
    if filter_by_namespace:
        try:
            # Get user's namespace prefix
            user_prefix_response = get_namespace_prefix()
            prefixes = [user_prefix_response.user_namespace_prefix]

            # Get all group namespace prefixes
            groups_response = get_my_groups()
            for group_name in groups_response.groups:
                tenant_prefix_response = get_namespace_prefix(tenant=group_name)
                prefixes.append(tenant_prefix_response.tenant_namespace_prefix)

            # Filter databases by any of the user's prefixes
            databases = [db for db in databases if db.startswith(tuple(prefixes))]
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
