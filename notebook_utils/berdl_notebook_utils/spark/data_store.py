"""Module for interacting with Iceberg catalog databases and tables.

This module provides functions to retrieve information about databases (namespaces),
tables, and their schemas from Iceberg catalogs via Spark SQL.

Iceberg catalogs use 3-level naming: catalog.namespace.table
- Personal catalog: ``my`` (e.g., ``my.demo.employees``)
- Tenant catalogs: ``globalusers``, ``kbase``, etc. (e.g., ``kbase.shared.dataset``)

Functions return "database" identifiers in ``catalog.namespace`` format
(e.g., ``my.demo``, ``globalusers.shared_data``), which can be used directly
in Spark SQL queries: ``SELECT * FROM {database}.{table}``.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import SparkSession

from berdl_notebook_utils.setup_spark_session import get_spark_session

logger = logging.getLogger(__name__)

# Catalogs to exclude from Iceberg listing (non-Iceberg catalogs)
_EXCLUDED_CATALOGS = {"spark_catalog"}

# Pattern to extract catalog name from spark.sql.catalog.<name> keys
# Matches top-level catalog registration keys only (no sub-properties)
_CATALOG_KEY_PATTERN = re.compile(r"^spark\.sql\.catalog\.([a-zA-Z_][a-zA-Z0-9_]*)$")


def _get_spark(spark: Optional[SparkSession] = None) -> SparkSession:
    """Get or create a SparkSession."""
    if spark is None:
        return get_spark_session()
    return spark


def _format_output(data: Any, return_json: bool = True) -> Union[str, Any]:
    """Format the output based on the return_json flag."""
    return json.dumps(data) if return_json else data


def _list_iceberg_catalogs(spark: SparkSession) -> List[str]:
    """List all Iceberg catalogs (excluding spark_catalog).

    In Spark 4.0 with Spark Connect, ``SHOW CATALOGS`` only returns catalogs
    registered in the client session's CatalogManager. Catalogs configured
    server-side (via ``spark-defaults.conf``) are accessible for queries but
    invisible to ``SHOW CATALOGS``.

    This function discovers catalogs by inspecting the Spark SQL configuration
    via the ``SET`` command, which returns all server-side configs through the
    Spark Connect gRPC channel. It looks for top-level
    ``spark.sql.catalog.<name>`` keys to identify registered catalogs.
    """
    rows = spark.sql("SET").collect()
    catalog_names = set()
    for row in rows:
        match = _CATALOG_KEY_PATTERN.match(row["key"])
        if match:
            catalog_names.add(match.group(1))
    logger.info(f"Discovered {len(catalog_names)} catalog(s) from Spark config: {sorted(catalog_names)}")
    return sorted(c for c in catalog_names if c not in _EXCLUDED_CATALOGS)


def get_databases(
    spark: Optional[SparkSession] = None,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    List all accessible Iceberg namespaces across all catalogs.

    Returns namespaces in ``catalog.namespace`` format (e.g., ``my.demo``,
    ``globalusers.shared_data``). These can be used directly in table references:
    ``SELECT * FROM {database}.{table}``.

    Args:
        spark: Optional SparkSession to use
        return_json: Whether to return JSON string or raw list

    Returns:
        Sorted list of ``catalog.namespace`` strings
    """
    spark = _get_spark(spark)
    catalogs = _list_iceberg_catalogs(spark)

    databases = []
    for catalog in catalogs:
        try:
            namespaces = spark.sql(f"SHOW NAMESPACES IN {catalog}").collect()
            for row in namespaces:
                databases.append(f"{catalog}.{row['namespace']}")
        except Exception:
            # Catalog may not be accessible or may have no namespaces
            pass

    return _format_output(sorted(databases), return_json)


def get_tables(
    database: str,
    spark: Optional[SparkSession] = None,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    List all tables in a specific Iceberg namespace.

    Args:
        database: Namespace in ``catalog.namespace`` format (e.g., ``my.demo``)
        spark: Optional SparkSession to use
        return_json: Whether to return JSON string or raw data

    Returns:
        List of table names
    """
    spark = _get_spark(spark)
    try:
        rows = spark.sql(f"SHOW TABLES IN {database}").collect()
        tables = sorted(row["tableName"] for row in rows)
    except Exception:
        tables = []

    return _format_output(tables, return_json)


def get_table_schema(
    database: str,
    table: str,
    spark: Optional[SparkSession] = None,
    return_json: bool = True,
) -> Union[str, List[str]]:
    """
    Get the column names of a specific table.

    Args:
        database: Namespace in ``catalog.namespace`` format (e.g., ``my.demo``)
        table: Name of the table
        spark: Optional SparkSession to use
        return_json: Whether to return JSON string or raw data

    Returns:
        List of column names
    """
    spark = _get_spark(spark)
    try:
        rows = spark.sql(f"DESCRIBE {database}.{table}").collect()
        # DESCRIBE returns col_name, data_type, comment — filter out partition/metadata rows
        columns = [row["col_name"] for row in rows if row["col_name"] and not row["col_name"].startswith("#")]
    except Exception:
        print(f"Error retrieving schema for table {table} in {database}")
        columns = []

    return _format_output(columns, return_json)


def get_db_structure(
    with_schema: bool = False,
    return_json: bool = True,
) -> Union[str, Dict]:
    """
    Get the structure of all accessible Iceberg namespaces.

    Args:
        with_schema: Whether to include table column names
        return_json: Whether to return the result as a JSON string

    Returns:
        Dictionary mapping ``catalog.namespace`` to table lists or schema dicts::

            {
                "my.demo": ["table1", "table2"],
                "globalusers.shared": ["dataset"]
            }

        Or with ``with_schema=True``::

            {
                "my.demo": {
                    "table1": ["col1", "col2"],
                    "table2": ["col1", "col2"]
                }
            }
    """
    spark = _get_spark()
    databases = get_databases(spark=spark, return_json=False)

    db_structure: Dict[str, Any] = {}
    for db in databases:
        tables = get_tables(database=db, spark=spark, return_json=False)
        if with_schema:
            db_structure[db] = {
                tbl: get_table_schema(database=db, table=tbl, spark=spark, return_json=False) for tbl in tables
            }
        else:
            db_structure[db] = tables

    return _format_output(db_structure, return_json)
