"""
Convenience wrapper functions for datalake-mcp-server operations.

This module provides high-level wrapper functions that make it easy to interact
with the datalake-mcp-server without needing to construct request/response objects.
All functions automatically handle authentication and return data in convenient Python types.
"""

import logging
from typing import Any

from datalake_mcp_server_client.api.delta_lake import (
    count_delta_table,
    get_database_structure,
    get_table_schema,
    list_database_tables,
    list_databases,
    query_delta_table,
    sample_delta_table,
    select_delta_table,
)
from datalake_mcp_server_client.models import (
    AggregationSpec,
    AggregationSpecFunction,
    ColumnSpec,
    DatabaseListRequest,
    DatabaseStructureRequest,
    ErrorResponse,
    FilterCondition,
    FilterConditionOperator,
    JoinClause,
    JoinClauseJoinType,
    OrderBySpec,
    OrderBySpecDirection,
    TableCountRequest,
    TableListRequest,
    TableQueryRequest,
    TableSampleRequest,
    TableSchemaRequest,
    TableSelectRequest,
)

from berdl_notebook_utils.mcp.client import get_datalake_mcp_client

logger = logging.getLogger(__name__)


def _handle_error_response(response: Any, operation: str) -> None:
    """
    Check if response is an error and raise an exception if so.

    Args:
        response: The response object from the API call
        operation: Description of the operation for error messages

    Raises:
        Exception: If the response is an ErrorResponse
    """
    if isinstance(response, ErrorResponse):
        error_msg = f"MCP Server error during {operation}: {response.error}, {response.error_type}, {response.message}"
        logger.error(error_msg)
        raise Exception(error_msg)


def mcp_list_databases(use_hms: bool = True) -> list[str]:
    """
    List all databases in the Hive metastore via MCP server.

    This function connects to the global datalake-mcp-server, which will use
    your authentication token to connect to your personal Spark Connect server.

    Args:
        use_hms: If True, uses Hive Metastore client for faster retrieval (default: True)

    Returns:
        List of database names

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> databases = mcp_list_databases()
        >>> print(databases)
        ['default', 'my_database', 'analytics']
    """
    client = get_datalake_mcp_client()
    request = DatabaseListRequest(use_hms=use_hms)

    logger.debug(f"Listing databases via MCP server (use_hms={use_hms})")
    response = list_databases.sync(client=client, body=request)

    _handle_error_response(response, "list_databases")

    if response is None:
        raise Exception("MCP Server returned no response for list_databases")

    logger.info(f"Retrieved {len(response.databases)} databases from MCP server")
    return response.databases


def mcp_list_tables(database: str, use_hms: bool = True) -> list[str]:
    """
    List all tables in a specific database via MCP server.

    Args:
        database: Name of the database
        use_hms: If True, uses Hive Metastore client for faster retrieval (default: True)

    Returns:
        List of table names in the database

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> tables = mcp_list_tables("my_database")
        >>> print(tables)
        ['users', 'orders', 'products']
    """
    client = get_datalake_mcp_client()
    request = TableListRequest(database=database, use_hms=use_hms)

    logger.debug(f"Listing tables in database '{database}' via MCP server")
    response = list_database_tables.sync(client=client, body=request)

    _handle_error_response(response, f"list_tables in database '{database}'")

    if response is None:
        raise Exception(f"MCP Server returned no response for list_tables in '{database}'")

    logger.info(f"Retrieved {len(response.tables)} tables from database '{database}'")
    return response.tables


def mcp_get_table_schema(database: str, table: str) -> list[str]:
    """
    Get the schema (column names) of a specific table via MCP server.

    Args:
        database: Name of the database
        table: Name of the table

    Returns:
        List of column names

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> columns = mcp_get_table_schema("my_database", "users")
        >>> print(columns)
        ['user_id', 'username', 'email', 'created_at']
    """
    client = get_datalake_mcp_client()
    request = TableSchemaRequest(database=database, table=table)

    logger.debug(f"Getting schema for table '{database}.{table}' via MCP server")
    response = get_table_schema.sync(client=client, body=request)

    _handle_error_response(response, f"get_table_schema for '{database}.{table}'")

    if response is None:
        raise Exception(f"MCP Server returned no response for get_table_schema '{database}.{table}'")

    logger.info(f"Retrieved schema for '{database}.{table}' with {len(response.columns)} columns")
    return response.columns


def mcp_get_database_structure(
    with_schema: bool = False, use_hms: bool = True
) -> dict[str, list[str] | dict[str, list[str]]]:
    """
    Get the complete structure of all databases via MCP server.

    Args:
        with_schema: If True, includes table schemas (column names) (default: False)
        use_hms: If True, uses Hive Metastore client for faster retrieval (default: True)

    Returns:
        Dictionary mapping database names to either:
        - List of table names (if with_schema=False)
        - Dictionary mapping table names to column lists (if with_schema=True)

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> # Without schema
        >>> structure = mcp_get_database_structure()
        >>> print(structure)
        {'default': ['table1', 'table2'], 'analytics': ['metrics', 'events']}

        >>> # With schema
        >>> structure = mcp_get_database_structure(with_schema=True)
        >>> print(structure)
        {'default': {'table1': ['col1', 'col2'], 'table2': ['col3', 'col4']}}
    """
    client = get_datalake_mcp_client()
    request = DatabaseStructureRequest(with_schema=with_schema, use_hms=use_hms)

    logger.debug(f"Getting database structure via MCP server (with_schema={with_schema})")
    response = get_database_structure.sync(client=client, body=request)

    _handle_error_response(response, "get_database_structure")

    if response is None:
        raise Exception("MCP Server returned no response for get_database_structure")

    # Convert Pydantic model to dict if needed
    structure = response.structure
    if hasattr(structure, "to_dict"):
        structure = structure.to_dict()
    elif hasattr(structure, "__dict__"):
        # For Pydantic v2, the structure might be a model instance
        structure = dict(structure) if not isinstance(structure, dict) else structure

    logger.info(f"Retrieved database structure with {len(structure)} databases")
    return structure


def mcp_count_table(database: str, table: str) -> int:
    """
    Count the number of rows in a Delta table via MCP server.

    Args:
        database: Name of the database
        table: Name of the table

    Returns:
        Total number of rows in the table

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> count = mcp_count_table("my_database", "users")
        >>> print(f"Table has {count} rows")
        Table has 1000000 rows
    """
    client = get_datalake_mcp_client()
    request = TableCountRequest(database=database, table=table)

    logger.debug(f"Counting rows in table '{database}.{table}' via MCP server")
    response = count_delta_table.sync(client=client, body=request)

    _handle_error_response(response, f"count_table for '{database}.{table}'")

    if response is None:
        raise Exception(f"MCP Server returned no response for count_table '{database}.{table}'")

    logger.info(f"Table '{database}.{table}' has {response.count} rows")
    return response.count


def mcp_sample_table(
    database: str,
    table: str,
    limit: int = 10,
    columns: list[str] | None = None,
    where_clause: str | None = None,
) -> list[dict[str, Any]]:
    """
    Sample data from a Delta table via MCP server.

    Args:
        database: Name of the database
        table: Name of the table
        limit: Maximum number of rows to return (default: 10)
        columns: List of column names to select (default: all columns)
        where_clause: Optional SQL WHERE clause to filter rows (without the WHERE keyword)

    Returns:
        List of dictionaries, where each dictionary represents a row

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> # Sample first 10 rows
        >>> rows = mcp_sample_table("my_database", "users")
        >>> print(rows[0])
        {'user_id': 1, 'username': 'alice', 'email': 'alice@example.com'}

        >>> # Sample with specific columns and filter
        >>> rows = mcp_sample_table(
        ...     "my_database", "users",
        ...     limit=5,
        ...     columns=["username", "email"],
        ...     where_clause="created_at > '2024-01-01'"
        ... )
    """
    client = get_datalake_mcp_client()
    request = TableSampleRequest(
        database=database, table=table, limit=limit, columns=columns, where_clause=where_clause
    )

    logger.debug(
        f"Sampling {limit} rows from table '{database}.{table}' via MCP server "
        f"(columns={columns}, where={where_clause})"
    )
    response = sample_delta_table.sync(client=client, body=request)

    _handle_error_response(response, f"sample_table for '{database}.{table}'")

    if response is None:
        raise Exception(f"MCP Server returned no response for sample_table '{database}.{table}'")

    logger.info(f"Retrieved {len(response.sample)} sample rows from '{database}.{table}'")
    return response.sample


def mcp_query_table(query: str) -> list[dict[str, Any]]:
    """
    Execute a SQL query against Delta tables via MCP server.

    This function allows you to run arbitrary SQL queries against your Delta Lake tables.
    The query is executed on your personal Spark Connect server via the MCP server.

    Args:
        query: SQL query to execute (e.g., "SELECT * FROM my_db.my_table LIMIT 10")

    Returns:
        List of dictionaries, where each dictionary represents a row from the query result

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> # Simple query
        >>> results = mcp_query_table("SELECT * FROM my_database.users LIMIT 5")
        >>> print(results[0])
        {'user_id': 1, 'username': 'alice', 'email': 'alice@example.com'}

        >>> # Complex query with joins and aggregations
        >>> results = mcp_query_table('''
        ...     SELECT u.username, COUNT(o.order_id) as order_count
        ...     FROM my_database.users u
        ...     LEFT JOIN my_database.orders o ON u.user_id = o.user_id
        ...     GROUP BY u.username
        ...     ORDER BY order_count DESC
        ...     LIMIT 10
        ... ''')
        >>> print(results)
        [{'username': 'alice', 'order_count': 42}, {'username': 'bob', 'order_count': 38}, ...]
    """
    client = get_datalake_mcp_client()
    request = TableQueryRequest(query=query)

    logger.debug(f"Executing query via MCP server: {query[:100]}...")
    response = query_delta_table.sync(client=client, body=request)

    _handle_error_response(response, "query_table")

    if response is None:
        raise Exception("MCP Server returned no response for query_table")

    logger.info(f"Query returned {len(response.result)} rows")
    return response.result


def mcp_select_table(
    database: str,
    table: str,
    columns: list[str | dict[str, Any]] | None = None,
    filters: list[dict[str, Any]] | None = None,
    aggregations: list[dict[str, Any]] | None = None,
    joins: list[dict[str, Any]] | None = None,
    group_by: list[str] | None = None,
    having: list[dict[str, Any]] | None = None,
    order_by: list[str | dict[str, Any]] | None = None,
    distinct: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Execute a structured SELECT query against a Delta table via MCP server.

    This function allows you to build complex queries without writing raw SQL.
    The backend constructs the query from the provided parameters, ensuring
    security and proper escaping of all values (preventing SQL injection).

    Args:
        database: Name of the primary database containing the table
        table: Name of the primary table to query
        columns: Columns to select. Can be:
            - None: SELECT * (all columns)
            - List of strings: ["col1", "col2"]
            - List of dicts for advanced options:
                - {"column": "col1", "alias": "c1"} (specify output alias)
                - {"column": "col1", "table_alias": "t", "alias": "c1"}
                  (specify table alias for disambiguation in joins)
        filters: WHERE clause conditions. List of dicts with:
            - column: Column name to filter on
            - operator: One of "=", "!=", "<", ">", "<=", ">=", "LIKE", "NOT LIKE",
                       "IN", "NOT IN", "BETWEEN", "IS NULL", "IS NOT NULL"
            - value: Value for comparison (for single-value operators)
            - values: List of values (for IN, NOT IN, BETWEEN)
        aggregations: Aggregation functions. List of dicts with:
            - function: One of "COUNT", "SUM", "AVG", "MIN", "MAX"
            - column: Column to aggregate (or "*" for COUNT(*))
            - alias: Optional output alias
        joins: JOIN clauses. List of dicts with:
            - join_type: One of "INNER", "LEFT", "RIGHT", "FULL"
            - database: Database of table to join
            - table: Table to join
            - on_left_column: Column from main table
            - on_right_column: Column from joined table
        group_by: List of column names for GROUP BY
        having: HAVING clause conditions (same format as filters, applied after GROUP BY)
        order_by: ORDER BY specification. Can be:
            - List of strings: ["col1", "col2"] (default ASC)
            - List of dicts: [{"column": "col1", "direction": "DESC"}]
        distinct: Whether to apply DISTINCT (default: False)
        limit: Maximum number of rows to return (default: 100)
        offset: Number of rows to skip for pagination (default: 0)

    Returns:
        Dictionary with:
        - data: List of row dictionaries
        - pagination: Dict with limit, offset, total_count, has_more

    Raises:
        Exception: If the MCP server returns an error or is unreachable

    Example:
        >>> # Simple select all columns
        >>> result = mcp_select_table("my_database", "users")
        >>> print(result["data"][:2])
        [{'user_id': 1, 'username': 'alice'}, {'user_id': 2, 'username': 'bob'}]

        >>> # Select specific columns with filter
        >>> result = mcp_select_table(
        ...     "my_database", "users",
        ...     columns=["username", "email"],
        ...     filters=[{"column": "created_at", "operator": ">", "value": "2024-01-01"}],
        ...     order_by=[{"column": "username", "direction": "ASC"}],
        ...     limit=10
        ... )

        >>> # Aggregation with GROUP BY
        >>> result = mcp_select_table(
        ...     "my_database", "orders",
        ...     columns=["status"],
        ...     aggregations=[{"function": "COUNT", "column": "*", "alias": "order_count"}],
        ...     group_by=["status"],
        ...     having=[{"column": "order_count", "operator": ">", "value": 10}]
        ... )

        >>> # JOIN example
        >>> result = mcp_select_table(
        ...     "my_database", "orders",
        ...     columns=[
        ...         {"column": "order_id"},
        ...         {"column": "username", "table_alias": "users"}
        ...     ],
        ...     joins=[{
        ...         "join_type": "LEFT",
        ...         "database": "my_database",
        ...         "table": "users",
        ...         "on_left_column": "user_id",
        ...         "on_right_column": "user_id"
        ...     }]
        ... )

        >>> # Pagination
        >>> page1 = mcp_select_table("db", "tbl", limit=10, offset=0)
        >>> if page1["pagination"]["has_more"]:
        ...     page2 = mcp_select_table("db", "tbl", limit=10, offset=10)
    """
    client = get_datalake_mcp_client()

    # Build columns spec
    columns_spec: list[ColumnSpec] | None = None
    if columns is not None:
        columns_spec = []
        for col in columns:
            if isinstance(col, str):
                columns_spec.append(ColumnSpec(column=col))
            else:
                columns_spec.append(
                    ColumnSpec(
                        column=col["column"],
                        table_alias=col.get("table_alias"),
                        alias=col.get("alias"),
                    )
                )

    # Build filters spec
    filters_spec: list[FilterCondition] | None = None
    if filters is not None:
        filters_spec = []
        for f in filters:
            filters_spec.append(
                FilterCondition(
                    column=f["column"],
                    operator=FilterConditionOperator(f["operator"]),
                    value=f.get("value"),
                    values=f.get("values"),
                )
            )

    # Build aggregations spec
    aggregations_spec: list[AggregationSpec] | None = None
    if aggregations is not None:
        aggregations_spec = []
        for agg in aggregations:
            aggregations_spec.append(
                AggregationSpec(
                    function=AggregationSpecFunction(agg["function"]),
                    column=agg["column"],
                    alias=agg.get("alias"),
                )
            )

    # Build joins spec
    joins_spec: list[JoinClause] | None = None
    if joins is not None:
        joins_spec = []
        for j in joins:
            joins_spec.append(
                JoinClause(
                    join_type=JoinClauseJoinType(j["join_type"]),
                    database=j["database"],
                    table=j["table"],
                    on_left_column=j["on_left_column"],
                    on_right_column=j["on_right_column"],
                )
            )

    # Build having spec
    having_spec: list[FilterCondition] | None = None
    if having is not None:
        having_spec = []
        for h in having:
            having_spec.append(
                FilterCondition(
                    column=h["column"],
                    operator=FilterConditionOperator(h["operator"]),
                    value=h.get("value"),
                    values=h.get("values"),
                )
            )

    # Build order_by spec
    order_by_spec: list[OrderBySpec] | None = None
    if order_by is not None:
        order_by_spec = []
        for ob in order_by:
            if isinstance(ob, str):
                order_by_spec.append(OrderBySpec(column=ob))
            else:
                order_by_spec.append(
                    OrderBySpec(
                        column=ob["column"],
                        direction=OrderBySpecDirection(ob.get("direction", "ASC")),
                    )
                )

    request = TableSelectRequest(
        database=database,
        table=table,
        columns=columns_spec,
        filters=filters_spec,
        aggregations=aggregations_spec,
        joins=joins_spec,
        group_by=group_by,
        having=having_spec,
        order_by=order_by_spec,
        distinct=distinct,
        limit=limit,
        offset=offset,
    )

    logger.debug(f"Executing structured SELECT on '{database}.{table}' via MCP server")
    response = select_delta_table.sync(client=client, body=request)

    _handle_error_response(response, f"select_table for '{database}.{table}'")

    if response is None:
        raise Exception(f"MCP Server returned no response for select_table '{database}.{table}'")

    # Convert data items to plain dicts
    data = [item.to_dict() for item in response.data]
    pagination = {
        "limit": response.pagination.limit,
        "offset": response.pagination.offset,
        "total_count": response.pagination.total_count,
        "has_more": response.pagination.has_more,
    }

    logger.info(
        f"Select query on '{database}.{table}' returned {len(data)} rows "
        f"(total: {pagination['total_count']}, has_more: {pagination['has_more']})"
    )

    return {"data": data, "pagination": pagination}
