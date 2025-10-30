"""
MCP (Model Context Protocol) integration for BERDL.

This module provides integration with the global datalake-mcp-server,
allowing users to query Delta Lake tables through a centralized MCP server
that connects to their personal Spark Connect server.
"""

from berdl_notebook_utils.mcp.client import get_datalake_mcp_client
from berdl_notebook_utils.mcp.operations import (
    mcp_count_table,
    mcp_get_database_structure,
    mcp_get_table_schema,
    mcp_list_databases,
    mcp_list_tables,
    mcp_query_table,
    mcp_sample_table,
)

__all__ = [
    "get_datalake_mcp_client",
    "mcp_list_databases",
    "mcp_list_tables",
    "mcp_get_table_schema",
    "mcp_get_database_structure",
    "mcp_count_table",
    "mcp_sample_table",
    "mcp_query_table",
]

