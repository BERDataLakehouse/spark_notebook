"""
MinIO Data Governance integration for BERDL notebooks

This package provides integration with the BERDL Data Governance API for managing
MinIO storage permissions, user workspaces, and data sharing in notebook environments.
"""

from .client import DataGovernanceClient
from .models import (
    CredentialsResponse,
    SharePathRequest,
    SharePathResponse,
    UnsharePathRequest,
    UnsharePathResponse,
    UserWorkspaceResponse,
    UserPoliciesResponse,
    HealthResponse,
    PathRequest,
    PublicAccessResponse,
    PathAccessInfoResponse,
    SqlWarehousePrefixResponse,
    GroupSqlWarehousePrefixResponse,
)
from .utils import (
    check_governance_health,
    get_minio_credentials,
    get_my_sql_warehouse,
    get_my_workspace,
    get_my_policies,
    get_table_access_info,
    share_table,
    unshare_table,
    make_table_public,
    make_table_private,
)
from .exceptions import DataGovernanceError, APIError

__all__ = [
    "DataGovernanceClient",
    "CredentialsResponse",
    "SharePathRequest",
    "SharePathResponse",
    "UnsharePathRequest",
    "UnsharePathResponse",
    "UserWorkspaceResponse",
    "UserPoliciesResponse",
    "HealthResponse",
    "PathRequest",
    "PublicAccessResponse",
    "PathAccessInfoResponse",
    "SqlWarehousePrefixResponse",
    "GroupSqlWarehousePrefixResponse",
    "check_governance_health",
    "get_minio_credentials",
    "get_my_sql_warehouse",
    "get_my_workspace",
    "get_my_policies",
    "get_table_access_info",
    "share_table",
    "unshare_table",
    "make_table_public",
    "make_table_private",
    "DataGovernanceError",
    "APIError",
]
