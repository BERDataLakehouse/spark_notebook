"""
Spark utilities for BERDL notebook environments.

This package provides comprehensive Spark utilities organized into focused modules:
- database: Catalog and namespace management utilities
- dataframe: DataFrame operations and display functions
- data_store: Hive metastore and database information utilities
- connect_server: Spark Connect server management

All functions are imported at the package level for convenient access.
"""

# Database utilities
from berdl_notebook_utils.spark.database import (
    create_namespace_if_not_exists,
    table_exists,
    remove_table,
    list_tables,
    list_namespaces,
    get_table_info,
)

# DataFrame utilities
from berdl_notebook_utils.spark.dataframe import (
    spark_to_pandas,
    display_df,
    display_namespace_viewer,
    read_csv,
)

# Cluster management utilities
from berdl_notebook_utils.spark.cluster import (
    check_api_health,
    get_cluster_status,
    create_cluster,
    delete_cluster,
)

# Data store utilities
from berdl_notebook_utils.spark.data_store import (
    get_databases,
    get_tables,
    get_table_schema,
    get_db_structure,
)

# Spark Connect server management
from berdl_notebook_utils.spark.connect_server import (
    start_spark_connect_server,
    get_spark_connect_status,
)


__all__ = [
    # Database operations
    "create_namespace_if_not_exists",
    "table_exists",
    "remove_table",
    "list_tables",
    "list_namespaces",
    "get_table_info",
    # DataFrame operations
    "spark_to_pandas",
    "display_df",
    "display_namespace_viewer",
    "read_csv",
    # Cluster management
    "check_api_health",
    "get_cluster_status",
    "create_cluster",
    "delete_cluster",
    # Data store operations
    "get_databases",
    "get_tables",
    "get_table_schema",
    "get_db_structure",
    # Spark Connect server management
    "start_spark_connect_server",
    "get_spark_connect_status",
]
