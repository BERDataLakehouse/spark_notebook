"""
MinIO Data Governance integration for BERDL notebooks

This package provides integration with the BERDL Data Governance API for managing
MinIO storage permissions, user workspaces, and data sharing in notebook environments.
"""

from .operations import (
    # Workspace/user info
    check_governance_health,
    get_group_sql_warehouse,
    get_minio_credentials,
    get_my_accessible_paths,
    get_my_groups,
    get_my_policies,
    get_my_sql_warehouse,
    get_my_workspace,
    get_namespace_prefix,
    # Management operations
    add_group_member,
    create_tenant_and_assign_users,
    list_groups,
    list_users,
    remove_group_member,
    # Table operations
    get_table_access_info,
    make_table_private,
    make_table_public,
    share_table,
    unshare_table,
)

__all__ = [
    # Workspace/user info
    "check_governance_health",
    "get_group_sql_warehouse",
    "get_minio_credentials",
    "get_my_accessible_paths",
    "get_my_groups",
    "get_my_policies",
    "get_my_sql_warehouse",
    "get_my_workspace",
    "get_namespace_prefix",
    # Management operations
    "add_group_member",
    "create_tenant_and_assign_users",
    "list_groups",
    "list_users",
    "remove_group_member",
    # Table operations
    "get_table_access_info",
    "make_table_private",
    "make_table_public",
    "share_table",
    "unshare_table",
]
