"""
Spark utilities for BERDL notebook environments.

This package provides comprehensive Spark utilities organized into focused modules:
- database: Catalog and namespace management utilities
- dataframe: DataFrame operations and display functions

All functions are imported at the package level for convenient access.
"""

# Database utilities
from .database import (
    create_namespace_if_not_exists,
    table_exists,
    remove_table,
    list_tables,
    list_namespaces,
    get_table_info,
)

# DataFrame utilities
from .dataframe import (
    spark_to_pandas,
    display_df,
    display_namespace_viewer,
    read_csv,
)

# Cluster management utilities
from .cluster import (
    check_api_health,
    get_cluster_status,
    create_cluster,
    delete_cluster,
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
    # Cluster management operations
    "check_api_health",
    "get_cluster_status",
    "create_cluster",
    "delete_cluster",
]
