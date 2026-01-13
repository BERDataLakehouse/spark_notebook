"""
BERDL Notebook Utilities Auto-Import
=====================================

This script automatically imports commonly used BERDL utilities into the IPython namespace
when a notebook kernel starts. All imports use # noqa F401 to suppress unused import warnings.

Organization:
1. Core Settings & Configuration
2. Client Factories
3. Spark Session Management
4. Spark Operations (Database, DataFrame, Cluster, Data Store)
5. MCP Server Operations
6. AI Agent
7. Data Governance & Sharing
8. Help Utilities
"""

# ============================================================================
# Core Settings & Configuration
# ============================================================================
from berdl_notebook_utils.berdl_settings import (  # noqa: F401
    BERDLSettings,
    get_settings,
)

# ============================================================================
# Client Factories
# ============================================================================
from berdl_notebook_utils.clients import (  # noqa: F401
    get_governance_client,
    get_hive_metastore_client,
    get_minio_client,
    get_spark_cluster_client,
    get_task_service_client,
)

# ============================================================================
# Spark Session Management
# ============================================================================
from berdl_notebook_utils.setup_spark_session import get_spark_session  # noqa: F401

# ============================================================================
# Spark Operations
# ============================================================================
from berdl_notebook_utils.spark import (  # noqa: F401
    # Cluster management
    check_api_health,
    create_cluster,
    delete_cluster,
    get_cluster_status,
    get_spark_connect_status,
    start_spark_connect_server,
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
    get_db_structure,
    get_table_schema,
    get_tables,
)

# ============================================================================
# MCP Server Operations
# ============================================================================
from berdl_notebook_utils.mcp import (  # noqa: F401
    mcp_count_table,
    mcp_get_database_structure,
    mcp_get_table_schema,
    mcp_list_databases,
    mcp_list_tables,
    mcp_query_table,
    mcp_sample_table,
)

# ============================================================================
# AI Agent (BERDL Assistant)
# ============================================================================
from berdl_notebook_utils.agent import (  # noqa: F401
    AgentSettings,
    BERDLAgent,
    create_berdl_agent,
    get_agent_settings,
)

# ============================================================================
# Data Governance & Sharing
# ============================================================================
from berdl_notebook_utils.minio_governance import (  # noqa: F401
    check_governance_health,
    create_tenant_and_assign_users,
    get_group_sql_warehouse,
    get_minio_credentials,
    get_my_groups,
    get_my_policies,
    get_my_sql_warehouse,
    get_my_workspace,
    get_namespace_prefix,
    get_table_access_info,
    make_table_private,
    make_table_public,
    list_available_groups,
    request_tenant_access,
    share_table,
    unshare_table,
)

# ============================================================================
# Help Utilities
# ============================================================================
from berdl_notebook_utils import berdl_notebook_help  # noqa: F401

# ============================================================================
# Data Lakehouse Ingest (config-driven ingestion)
# ============================================================================
from data_lakehouse_ingest import ingest  # noqa: F401
