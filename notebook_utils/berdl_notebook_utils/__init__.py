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
)
from berdl_notebook_utils.agent import (
    # Agent
    create_berdl_agent,
    BERDLAgent,
    AgentSettings,
    get_agent_settings,
)

__all__ = [
    "BERDLSettings",
    "get_settings",
    "validate_environment",
    "get_minio_client",
    "get_task_service_client",
    "get_governance_client",
    "get_spark_cluster_client",
    "get_spark_session",
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
    # Agent
    "create_berdl_agent",
    "BERDLAgent",
    "AgentSettings",
    "get_agent_settings",
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

    MCP Server Operations (via Global Datalake MCP Server):
    -------------------------------------------------------
    - mcp_list_databases: List all databases via MCP server
    - mcp_list_tables: List tables in a database via MCP server
    - mcp_get_table_schema: Get table schema via MCP server
    - mcp_get_database_structure: Get complete database structure via MCP server
    - mcp_count_table: Count rows in a table via MCP server
    - mcp_sample_table: Sample data from a table via MCP server
    - mcp_query_table: Execute SQL queries via MCP server

    BERDL Agent (AI Assistant):
    ---------------------------
    - create_berdl_agent: Create an AI agent for natural language data lake interactions
    - BERDLAgent: Agent class for advanced configuration
    - AgentSettings: Agent configuration settings

    Usage:
    ------
    from berdl_notebook_utils import get_spark_session, display_df
    from berdl_notebook_utils.minio_governance import share_table

    # Create Spark session
    spark = get_spark_session("MyAnalysis")

    # Read and display data
    df = spark.read.csv("s3a://your-bucket/data.csv")
    display_df(df)

    # Share table with colleagues
    share_table("analytics", "user_metrics", with_users=["alice", "bob"])

    # Use MCP server for queries
    from berdl_notebook_utils import mcp_list_databases, mcp_query_table

    databases = mcp_list_databases()
    results = mcp_query_table("SELECT * FROM my_db.my_table LIMIT 10")

    # Use AI Agent for natural language queries
    from berdl_notebook_utils import create_berdl_agent

    agent = create_berdl_agent()
    result = agent.run("What databases do I have access to?")
    result = agent.run("Show me the top 10 rows from my_db.user_activity")

    For detailed documentation, see the README.md or individual module docstrings.
    """
    )
