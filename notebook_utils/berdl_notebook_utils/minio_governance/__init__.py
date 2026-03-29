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
    get_polaris_credentials,
    rotate_minio_credentials,
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
    list_user_names,
    list_users,
    remove_group_member,
    # Table operations
    get_table_access_info,
    make_table_private,
    make_table_public,
    share_table,
    unshare_table,
    # Tenant access requests
    list_available_groups,
    request_tenant_access,
    # Migration (admin-only)
    ensure_polaris_resources,
    regenerate_policies,
)
from .tenant_management import (
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

__all__ = [
    # Workspace/user info
    "check_governance_health",
    "get_group_sql_warehouse",
    "get_minio_credentials",
    "get_polaris_credentials",
    "rotate_minio_credentials",
    "get_my_accessible_paths",
    "get_my_groups",
    "get_my_policies",
    "get_my_sql_warehouse",
    "get_my_workspace",
    "get_namespace_prefix",
    # Management operations (admin-only, via /management API)
    "add_group_member",
    "create_tenant_and_assign_users",
    "list_groups",
    "list_user_names",
    "list_users",
    "regenerate_policies",
    "remove_group_member",
    # Tenant management (steward or admin, via /tenants API)
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
    # Table operations
    "get_table_access_info",
    "make_table_private",
    "make_table_public",
    "share_table",
    "unshare_table",
    # Tenant access requests
    "list_available_groups",
    "request_tenant_access",
    # Migration (admin-only)
    "ensure_polaris_resources",
    "regenerate_policies",
]
