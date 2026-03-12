"""
LangChain tools wrapping BERDL MCP operations.

This module provides tool definitions that allow the agent to interact with
the BERDL data lake through the MCP server.

All tools follow these conventions:
- Use StructuredTool with Pydantic schemas for multi-parameter functions
- Use Tool with single string parameter for simple operations
- Consistent error handling with user-friendly messages
- JSON formatting for structured results
"""

import json
import logging
from typing import Any

from langchain.tools import Tool
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

from berdl_notebook_utils.agent.prompts import TOOL_DESCRIPTIONS
from berdl_notebook_utils.agent.settings import get_agent_settings
from berdl_notebook_utils.mcp import operations as mcp_ops

logger = logging.getLogger(__name__)


def _format_result(result: Any) -> str:
    """
    Format tool results as strings for the agent.

    Args:
        result: Result from MCP or governance operation

    Returns:
        Formatted string representation suitable for LLM consumption
    """
    if isinstance(result, (list, dict)):
        # Pretty print JSON for structured data
        return json.dumps(result, indent=2, default=str)
    return str(result)


# =============================================================================
# Pydantic Input Schemas
# =============================================================================


class TableIdentifierInput(BaseModel):
    """Input schema for operations that require database and table names."""

    database: str = Field(description="Database name (e.g., 'u_tgu2__demo_personal')")
    table: str = Field(description="Table name (e.g., 'personal_test_table')")


class SampleTableInput(BaseModel):
    """Input schema for sampling table data with optional filters."""

    database: str = Field(description="Database name (e.g., 'u_tgu2__demo_personal')")
    table: str = Field(description="Table name (e.g., 'personal_test_table')")
    limit: int = Field(default=10, description="Maximum number of rows to return (default: 10)")
    columns: list[str] | None = Field(default=None, description="Specific columns to select (optional)")
    where_clause: str | None = Field(
        default=None, description="SQL WHERE clause filter without WHERE keyword (optional)"
    )


# =============================================================================
# Discovery Tools
# =============================================================================


def list_databases(dummy_input: str = "") -> str:
    """
    List all databases accessible to the user.

    Args:
        dummy_input: Unused parameter (LangChain Tool requires string input)

    Returns:
        JSON-formatted list of database names or error message
    """
    try:
        databases = mcp_ops.mcp_list_databases(use_hms=True)
        if not databases:
            return "No databases found. You may need to create a database first."
        return _format_result(databases)
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        return f"Error listing databases: {e}"


def list_tables(database: str) -> str:
    """
    List all tables in a specific database.

    Args:
        database: Name of the database to query

    Returns:
        JSON-formatted list of table names or error message
    """
    try:
        tables = mcp_ops.mcp_list_tables(database=database, use_hms=True)
        if not tables:
            return f"No tables found in database '{database}'. The database may be empty."
        return _format_result(tables)
    except Exception as e:
        logger.error(f"Error listing tables in '{database}': {e}")
        return f"Error listing tables in '{database}': {e}"


def get_table_schema(database: str, table: str) -> str:
    """
    Get the schema (column names and types) for a specific table.

    Args:
        database: Name of the database
        table: Name of the table

    Returns:
        JSON-formatted list of column definitions or error message
    """
    try:
        schema = mcp_ops.mcp_get_table_schema(database=database, table=table)
        return _format_result(schema)
    except Exception as e:
        logger.error(f"Error getting schema for '{database}.{table}': {e}")
        return f"Error getting table schema for '{database}.{table}': {e}"


def get_database_structure(input_str: str = "false") -> str:
    """
    Get the complete structure of all databases and tables.

    Args:
        input_str: "true" or "false" string indicating whether to include schemas

    Returns:
        JSON-formatted database structure or error message
    """
    try:
        # Parse boolean input
        with_schema = input_str.lower() in ("true", "1", "yes")
        structure = mcp_ops.mcp_get_database_structure(with_schema=with_schema, use_hms=True)
        return _format_result(structure)
    except Exception as e:
        logger.error(f"Error getting database structure: {e}")
        return f"Error getting database structure: {e}"


# =============================================================================
# Data Inspection Tools
# =============================================================================


def sample_table(
    database: str,
    table: str,
    limit: int = 10,
    columns: list[str] | None = None,
    where_clause: str | None = None,
) -> str:
    """
    Sample data from a Delta Lake table.

    Args:
        database: Name of the database
        table: Name of the table
        limit: Maximum number of rows to return (default: 10)
        columns: Optional list of column names to select
        where_clause: Optional SQL WHERE clause (without WHERE keyword)

    Returns:
        JSON-formatted sample data or error message
    """
    try:
        result = mcp_ops.mcp_sample_table(
            database=database,
            table=table,
            limit=limit,
            columns=columns,
            where_clause=where_clause,
        )
        return _format_result(result)
    except Exception as e:
        logger.error(f"Error sampling table '{database}.{table}': {e}")
        return f"Error sampling table '{database}.{table}': {e}"


def count_table_rows(database: str, table: str) -> str:
    """
    Count the number of rows in a Delta Lake table.

    Args:
        database: Name of the database
        table: Name of the table

    Returns:
        Human-readable row count message or error message
    """
    try:
        count = mcp_ops.mcp_count_table(database=database, table=table)
        return f"Table {database}.{table} has {count:,} rows."
    except Exception as e:
        logger.error(f"Error counting rows in '{database}.{table}': {e}")
        return f"Error counting rows in '{database}.{table}': {e}"


# =============================================================================
# Query Tools
# =============================================================================


def query_table(query: str) -> str:
    """
    Execute a SQL query against Delta Lake tables.

    Args:
        query: SQL query string (must be valid Spark SQL)

    Returns:
        JSON-formatted query results or error message
    """
    try:
        # Check row limit from settings
        settings = get_agent_settings()

        # Add basic validation
        if not query.strip():
            return "Error: Query cannot be empty."

        # Warn if no LIMIT clause and settings enforce limits
        if "limit" not in query.lower() and settings.AGENT_SQL_ROW_LIMIT:
            return (
                f"Warning: Your query doesn't have a LIMIT clause. "
                f"Please add 'LIMIT {settings.AGENT_SQL_ROW_LIMIT}' to avoid overwhelming results. "
                f"Modified query suggestion: {query.rstrip(';')} LIMIT {settings.AGENT_SQL_ROW_LIMIT};"
            )

        result = mcp_ops.mcp_query_table(query=query)

        # Check if result is too large
        if isinstance(result, list) and len(result) > 100:
            # Show first 100 rows and summary
            summary = f"\nShowing first 100 of {len(result)} total rows. Use LIMIT clause to control output size."
            return _format_result(result[:100]) + summary

        return _format_result(result)
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return f"Error executing query: {e}"


# =============================================================================
# Tool Definitions
# =============================================================================


def get_discovery_tools() -> list[Tool | StructuredTool]:
    """
    Get data discovery tools for exploring databases and tables.

    Returns:
        List of discovery tools (mix of Tool and StructuredTool)
    """
    return [
        Tool(
            name="list_databases",
            func=list_databases,
            description=TOOL_DESCRIPTIONS["list_databases"],
        ),
        Tool(
            name="list_tables",
            func=list_tables,
            description=TOOL_DESCRIPTIONS["list_tables"],
        ),
        StructuredTool(
            name="get_table_schema",
            description=TOOL_DESCRIPTIONS["get_table_schema"],
            func=get_table_schema,
            args_schema=TableIdentifierInput,
        ),
        Tool(
            name="get_database_structure",
            func=get_database_structure,
            description=TOOL_DESCRIPTIONS["get_database_structure"],
        ),
    ]


def get_data_inspection_tools() -> list[StructuredTool]:
    """
    Get data inspection tools for sampling and counting table data.

    Returns:
        List of data inspection tools
    """
    return [
        StructuredTool(
            name="sample_table",
            description=TOOL_DESCRIPTIONS["sample_table"],
            func=sample_table,
            args_schema=SampleTableInput,
        ),
        StructuredTool(
            name="count_table_rows",
            description=TOOL_DESCRIPTIONS["count_table_rows"],
            func=count_table_rows,
            args_schema=TableIdentifierInput,
        ),
    ]


def get_query_tools() -> list[Tool]:
    """
    Get SQL query execution tools.

    Returns:
        List of query execution tools
    """
    return [
        Tool(
            name="query_table",
            func=query_table,
            description=TOOL_DESCRIPTIONS["query_table"],
        ),
    ]


def get_all_tools(enable_sql_execution: bool = True) -> list[Tool | StructuredTool]:
    """
    Get all available tools for the agent.

    Args:
        enable_sql_execution: Include SQL query execution tools

    Returns:
        List of LangChain Tool objects (mix of Tool and StructuredTool)
    """
    tools = []

    # Always include discovery and inspection tools
    tools.extend(get_discovery_tools())
    tools.extend(get_data_inspection_tools())

    # Optionally include query tools
    if enable_sql_execution:
        tools.extend(get_query_tools())

    return tools
