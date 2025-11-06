"""
LangChain tools wrapping BERDL MCP operations.

This module provides tool definitions that allow the agent to interact with
the BERDL data lake through the MCP server.
"""

import json
from typing import Any

from langchain.tools import Tool

from berdl_notebook_utils.agent.prompts import TOOL_DESCRIPTIONS
from berdl_notebook_utils.agent.settings import get_agent_settings
from berdl_notebook_utils.mcp import operations as mcp_ops


def _format_result(result: Any) -> str:
    """
    Format tool results as strings for the agent.

    Args:
        result: Result from MCP or governance operation

    Returns:
        Formatted string representation
    """
    if isinstance(result, (list, dict)):
        # Pretty print JSON for structured data
        return json.dumps(result, indent=2, default=str)
    return str(result)


# =============================================================================
# Discovery Tools
# =============================================================================


def list_databases_wrapper(dummy_input: str = "") -> str:
    """Wrapper for mcp_list_databases that accepts string input (LangChain requirement)."""
    databases = mcp_ops.mcp_list_databases(use_hms=True)
    if not databases:
        return "No databases found. You may need to create a database first."
    return _format_result(databases)


def list_tables_wrapper(database: str) -> str:
    """Wrapper for mcp_list_tables."""
    tables = mcp_ops.mcp_list_tables(database=database, use_hms=True)
    if not tables:
        return f"No tables found in database '{database}'. The database may be empty."
    return _format_result(tables)


def get_table_schema_wrapper(input_str: str) -> str:
    """
    Wrapper for mcp_get_table_schema.

    Input format: "database.table" or JSON: {"database": "db", "table": "tbl"}
    """
    try:
        # Try parsing as JSON first
        if input_str.startswith("{"):
            params = json.loads(input_str)
            database = params["database"]
            table = params["table"]
        else:
            # Parse "database.table" format
            parts = input_str.split(".", 1)
            if len(parts) != 2:
                return f"Error: Invalid format. Use 'database.table' or JSON. Got: {input_str}"
            database, table = parts

        schema = mcp_ops.mcp_get_table_schema(database=database, table=table)
        return _format_result(schema)
    except json.JSONDecodeError as e:
        return f"Error parsing input: {e}"
    except KeyError as e:
        return f"Error: Missing required key {e}"
    except Exception as e:
        return f"Error getting table schema: {e}"


def get_database_structure_wrapper(input_str: str = "false") -> str:
    """
    Wrapper for mcp_get_database_structure.

    Input: "true" or "false" (string) for with_schema parameter.
    """
    try:
        # Parse boolean input
        with_schema = input_str.lower() in ("true", "1", "yes")
        structure = mcp_ops.mcp_get_database_structure(with_schema=with_schema, use_hms=True)
        return _format_result(structure)
    except Exception as e:
        return f"Error getting database structure: {e}"


# =============================================================================
# Data Inspection Tools
# =============================================================================


def sample_table_wrapper(input_str: str) -> str:
    """
    Wrapper for mcp_sample_table.

    Input format: JSON with keys: database, table, limit (optional), columns (optional), where_clause (optional)
    Example: {"database": "mydb", "table": "mytable", "limit": 10}
    """
    try:
        params = json.loads(input_str)
        database = params["database"]
        table = params["table"]
        limit = params.get("limit", 10)
        columns = params.get("columns")
        where_clause = params.get("where_clause")

        result = mcp_ops.mcp_sample_table(
            database=database,
            table=table,
            limit=limit,
            columns=columns,
            where_clause=where_clause,
        )
        return _format_result(result)
    except json.JSONDecodeError as e:
        return f"Error parsing input JSON: {e}"
    except KeyError as e:
        return f"Error: Missing required key {e}. Need at least 'database' and 'table'."
    except Exception as e:
        return f"Error sampling table: {e}"


def count_table_rows_wrapper(input_str: str) -> str:
    """
    Wrapper for mcp_count_table.

    Input format: "database.table" or JSON: {"database": "db", "table": "tbl"}
    """
    try:
        # Try parsing as JSON first
        if input_str.startswith("{"):
            params = json.loads(input_str)
            database = params["database"]
            table = params["table"]
        else:
            # Parse "database.table" format
            parts = input_str.split(".", 1)
            if len(parts) != 2:
                return f"Error: Invalid format. Use 'database.table' or JSON. Got: {input_str}"
            database, table = parts

        count = mcp_ops.mcp_count_table(database=database, table=table)
        return f"Table {database}.{table} has {count:,} rows."
    except json.JSONDecodeError as e:
        return f"Error parsing input: {e}"
    except KeyError as e:
        return f"Error: Missing required key {e}"
    except Exception as e:
        return f"Error counting rows: {e}"


# =============================================================================
# Query Tools
# =============================================================================


def query_table_wrapper(query: str) -> str:
    """
    Wrapper for mcp_query_table.

    Input: SQL query string.
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
        return f"Error executing query: {e}"


# =============================================================================
# Tool Definitions
# =============================================================================


def get_discovery_tools() -> list[Tool]:
    """Get data discovery tools."""
    return [
        Tool(
            name="list_databases",
            func=list_databases_wrapper,
            description=TOOL_DESCRIPTIONS["list_databases"],
        ),
        Tool(
            name="list_tables",
            func=list_tables_wrapper,
            description=TOOL_DESCRIPTIONS["list_tables"],
        ),
        Tool(
            name="get_table_schema",
            func=get_table_schema_wrapper,
            description=TOOL_DESCRIPTIONS["get_table_schema"],
        ),
        Tool(
            name="get_database_structure",
            func=get_database_structure_wrapper,
            description=TOOL_DESCRIPTIONS["get_database_structure"],
        ),
    ]


def get_data_inspection_tools() -> list[Tool]:
    """Get data inspection tools."""
    return [
        Tool(
            name="sample_table",
            func=sample_table_wrapper,
            description=TOOL_DESCRIPTIONS["sample_table"],
        ),
        Tool(
            name="count_table_rows",
            func=count_table_rows_wrapper,
            description=TOOL_DESCRIPTIONS["count_table_rows"],
        ),
    ]


def get_query_tools() -> list[Tool]:
    """Get SQL query execution tools."""
    return [
        Tool(
            name="query_table",
            func=query_table_wrapper,
            description=TOOL_DESCRIPTIONS["query_table"],
        ),
    ]


def get_all_tools(enable_sql_execution: bool = True) -> list[Tool]:
    """
    Get all available tools for the agent.

    Args:
        enable_sql_execution: Include SQL query execution tools

    Returns:
        List of LangChain Tool objects
    """
    tools = []

    # Always include discovery and inspection tools
    tools.extend(get_discovery_tools())
    tools.extend(get_data_inspection_tools())

    # Optionally include query tools
    if enable_sql_execution:
        tools.extend(get_query_tools())

    return tools
