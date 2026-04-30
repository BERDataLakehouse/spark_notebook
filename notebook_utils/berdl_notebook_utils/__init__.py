from berdl_notebook_utils.berdl_settings import (
    BERDLSettings,
    get_settings,
    validate_environment,
)
from berdl_notebook_utils.clients import (
    get_governance_client,
    get_minio_client,
    get_spark_cluster_client,
    get_task_service_client,
    get_hive_metastore_client,
)
from berdl_notebook_utils.setup_spark_session import get_spark_session
from berdl_notebook_utils.setup_trino_session import get_trino_connection
from berdl_notebook_utils.spark import (
    # Cluster management
    check_api_health,
    create_cluster,
    delete_cluster,
    get_cluster_status,
    # Database operations
    create_namespace_if_not_exists,
    get_table_info,
    list_namespaces,
    list_tables,
    remove_table,
    table_exists,
    # DataFrame operations
    display_df,
    display_namespace_viewer,
    read_csv,
    spark_to_pandas,
    # Data store operations
    get_databases,
    get_tables,
    get_table_schema,
    get_db_structure,
)
from berdl_notebook_utils.minio_governance.tenant_management import (
    add_tenant_member,
    assign_steward,
    get_my_steward_tenants,
    get_tenant_detail,
    get_tenant_members,
    get_tenant_stewards,
    list_tenants,
    remove_steward,
    remove_tenant_member,
    show_my_tenants,
    update_tenant_metadata,
)
from berdl_notebook_utils.mcp import (
    # MCP Client
    get_datalake_mcp_client,
    # MCP Database operations
    mcp_list_databases,
    mcp_list_tables,
    mcp_get_table_schema,
    mcp_get_database_structure,
    # MCP Query operations
    mcp_count_table,
    mcp_sample_table,
    mcp_query_table,
    mcp_select_table,
)
from berdl_notebook_utils.refresh import refresh_spark_environment

# Agent imports are lazy-loaded via __getattr__ below to avoid ImportError
# when a user's venv has an incompatible langchain version that shadows the
# system package. This prevents the agent module from breaking all other imports.
_AGENT_NAMES = {"create_berdl_agent", "BERDLAgent", "AgentSettings", "get_agent_settings"}


def __getattr__(name):
    if name in _AGENT_NAMES:
        from berdl_notebook_utils.agent import (
            create_berdl_agent,
            BERDLAgent,
            AgentSettings,
            get_agent_settings,
        )

        _agent_exports = {
            "create_berdl_agent": create_berdl_agent,
            "BERDLAgent": BERDLAgent,
            "AgentSettings": AgentSettings,
            "get_agent_settings": get_agent_settings,
        }
        # Cache in module globals so __getattr__ is only called once
        globals().update(_agent_exports)
        return _agent_exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "BERDLSettings",
    "get_settings",
    "validate_environment",
    "get_minio_client",
    "get_task_service_client",
    "get_governance_client",
    "get_spark_cluster_client",
    "get_spark_session",
    "get_trino_connection",
    "get_hive_metastore_client",
    # Database operations
    "create_namespace_if_not_exists",
    "table_exists",
    "remove_table",
    "list_tables",
    "list_namespaces",
    "get_table_info",
    # DataFrame operations
    "display_df",
    "spark_to_pandas",
    "read_csv",
    "display_namespace_viewer",
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
    # Tenant management (steward or admin)
    "add_tenant_member",
    "assign_steward",
    "get_my_steward_tenants",
    "get_tenant_detail",
    "get_tenant_members",
    "get_tenant_stewards",
    "list_tenants",
    "remove_steward",
    "remove_tenant_member",
    "show_my_tenants",
    "update_tenant_metadata",
    # MCP Client
    "get_datalake_mcp_client",
    # MCP Database operations
    "mcp_list_databases",
    "mcp_list_tables",
    "mcp_get_table_schema",
    "mcp_get_database_structure",
    # MCP Query operations
    "mcp_count_table",
    "mcp_sample_table",
    "mcp_query_table",
    "mcp_select_table",
    # Agent exports are intentionally omitted from __all__ to avoid eager
    # imports when langchain is incompatible, but they remain available via
    # lazy top-level access through __getattr__. Use:
    # from berdl_notebook_utils import create_berdl_agent
    # Environment refresh
    "refresh_spark_environment",
]


def berdl_notebook_help():
    print(
        """
    berdl_notebook_utils
    ====================

    A collection of utilities for working with Spark, MinIO, data governance, and other BERDL services
    in a Jupyter notebook environment.

    Client Functions:
    -----------------
    - get_minio_client: MinIO S3 client instance
    - get_task_service_client: CDM Task Service client instance
    - get_governance_client: Data Governance API client instance
    - get_spark_cluster_client: Spark Cluster Manager API client instance
    - get_spark_session: Create configured Spark session with Delta Lake and S3 support
    - get_trino_connection: Create Trino connection with per-user dynamic catalog
    - get_datalake_mcp_client: Datalake MCP Server client instance

    Database Operations:
    -------------------
    - create_namespace_if_not_exists: Create database namespace
    - table_exists: Check if table exists
    - remove_table: Delete table
    - list_tables: List tables in namespace
    - list_namespaces: List available namespaces
    - get_table_info: Get table metadata

    DataFrame Utilities:
    -------------------
    - display_df: Display pandas/Spark DataFrames with interactive tables
    - spark_to_pandas: Convert Spark DataFrame to pandas
    - read_csv: S3-aware CSV reader with auto-detection
    - display_namespace_viewer: Interactive namespace browser

    Cluster Management:
    ------------------
    - check_api_health: Check Spark Cluster Manager API health
    - get_cluster_status: Get current cluster status
    - create_cluster: Create new Spark cluster
    - delete_cluster: Delete Spark cluster

    Tenant Management (steward or admin, via /tenants API):
    ---------------------------------------------------
    - list_tenants: List all tenants with summary info
    - get_my_steward_tenants: List tenants where you are a steward
    - get_tenant_detail: Get full tenant detail (metadata, members, stewards)
    - get_tenant_members: List tenant members with profiles
    - get_tenant_stewards: List tenant stewards with profiles
    - add_tenant_member: Add a user to a tenant
    - remove_tenant_member: Remove a user from a tenant
    - assign_steward: Assign a user as steward of a tenant (admin only)
    - remove_steward: Remove steward assignment from a tenant (admin only)
    - update_tenant_metadata: Update tenant display name, description, org
    - show_my_tenants: Display all your tenants (admins see all tenants)

    Data Store Operations:
    ---------------------
    - get_databases: List all accessible databases
    - get_tables: List tables in a database
    - get_table_schema: Get column schema for a table
    - get_db_structure: Get full {database: [tables]} structure (optionally with schemas)

    Environment Refresh:
    -------------------
    - refresh_spark_environment: Re-fetch credentials and restart Spark

    Usage:
    ------
    from berdl_notebook_utils import get_spark_session, display_df
    from berdl_notebook_utils.minio_governance import share_table

    # Create Spark session
    spark = get_spark_session("MyAnalysis")

    # Browse the data store
    from berdl_notebook_utils import get_databases, get_tables, get_table_schema

    databases = get_databases()
    tables = get_tables("my_db")
    schema = get_table_schema("my_db", "my_table")

    # Run SQL queries through Spark
    results = spark.sql("SELECT * FROM my_db.my_table LIMIT 10").toPandas()

    For detailed documentation, see the README.md or individual module docstrings.
    """
    )
