"""
Utility functions for BERDL MinIO Data Governance integration
"""

import logging
import os
from typing import List, Optional

from berdl_notebook_utils import get_settings
from berdl_notebook_utils.clients import get_governance_client
from governance_client.api.credentials import get_credentials_credentials_get
from governance_client.api.health import health_check_health_get
from governance_client.api.sharing import (
    get_path_access_info_sharing_get_path_access_info_post,
    make_path_private_sharing_make_private_post,
    make_path_public_sharing_make_public_post,
    share_data_sharing_share_post,
    unshare_data_sharing_unshare_post,
)
from governance_client.api.workspaces import (
    get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get,
    get_my_policies_workspaces_me_policies_get,
    get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get,
    get_my_workspace_workspaces_me_get,
)
from governance_client.models import (
    CredentialsResponse,
    HealthResponse,
    PathAccessResponse,
    PathRequest,
    PublicAccessResponse,
    ShareRequest,
    ShareResponse,
    UnshareRequest,
    UnshareResponse,
    UserPoliciesResponse,
    UserSqlWarehousePrefixResponse,
)

# =============================================================================
# CONSTANTS
# =============================================================================

# Default BERDL storage configuration
SQL_WAREHOUSE_BUCKET = "cdm-lake"  # TODO: change to berdl-lake
SQL_USER_WAREHOUSE_PATH = "users-sql-warehouse"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _build_table_path(username: str, namespace: str, table_name: str) -> str:
    """
    Build S3 path for a SQL warehouse table.

    Args:
        username: User's username
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name

    Returns:
        Full S3 path to the table
    """
    # Ensure namespace ends with .db
    if not namespace.endswith(".db"):
        namespace = f"{namespace}.db"

    return f"s3a://{SQL_WAREHOUSE_BUCKET}/{SQL_USER_WAREHOUSE_PATH}/{username}/{namespace}/{table_name}"


def check_governance_health() -> HealthResponse:
    """
    Check data governance service health.

    Returns:
        HealthResponse with service status
    """
    client = get_governance_client()
    return health_check_health_get.sync(client=client)


def get_minio_credentials() -> CredentialsResponse:
    """
    Get MinIO credentials for the current user and set them as environment variables.

    Sets the following environment variables:
    - MINIO_ACCESS_KEY: User's MinIO access key
    - MINIO_SECRET_KEY: User's MinIO secret key

    Returns:
        CredentialsResponse with username, access_key, and secret_key
    """
    client = get_governance_client()
    credentials = get_credentials_credentials_get.sync(client=client)

    # Set MinIO credentials as environment variables
    if os.environ.get("USE_DATA_GOVERNANCE_CREDENTIALS", "false") == "true":
        os.environ["MINIO_ACCESS_KEY"] = credentials.access_key
        os.environ["MINIO_SECRET_KEY"] = credentials.secret_key
    else:
        credentials.access_key = str(os.environ.get("MINIO_ACCESS_KEY"))
        credentials.secret_key = str(os.environ.get("MINIO_SECRET_KEY"))

    return credentials


def get_my_sql_warehouse() -> UserSqlWarehousePrefixResponse:
    """
    Get SQL warehouse prefix for the current user.

    Returns:
        UserSqlWarehousePrefixResponse with username and sql_warehouse_prefix
    """
    client = get_governance_client()
    return get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get.sync(client=client)


def get_group_sql_warehouse(group_name: str):
    """
    Get SQL warehouse prefix for a specific group.

    Args:
        group_name: Name of the group

    Returns:
        GroupSqlWarehousePrefixResponse with group_name and sql_warehouse_prefix
    """
    client = get_governance_client()
    return get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get.sync(
        client=client, group_name=group_name
    )


def get_my_workspace():
    """
    Get comprehensive workspace information for the current user.

    Returns:
        Workspace information from governance client
    """
    client = get_governance_client()
    return get_my_workspace_workspaces_me_get.sync(client=client)


def get_my_policies() -> UserPoliciesResponse:
    """
    Get detailed policy information for the current user.

    Returns:
        UserPoliciesResponse with user_home_policy, user_system_policy, and group_policies
    """
    client = get_governance_client()
    return get_my_policies_workspaces_me_policies_get.sync(client=client)


def get_table_access_info(namespace: str, table_name: str) -> PathAccessResponse:
    """
    Get access information for a SQL warehouse table.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")
    """
    client = get_governance_client()
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = PathRequest(path=table_path)
    return get_path_access_info_sharing_get_path_access_info_post.sync(client=client, body=request)


# =============================================================================
# TABLE-SPECIFIC FUNCTIONS - For SQL Warehouse tables
# =============================================================================


def share_table(
    namespace: str,
    table_name: str,
    with_users: Optional[List[str]] = None,
    with_groups: Optional[List[str]] = None,
) -> ShareResponse:
    """
    Share a SQL warehouse table with users and/or groups.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")
        with_users: List of usernames to share with
        with_groups: List of group names to share with

    Returns:
        SharePathResponse with sharing details and success/error information

    Example:
        share_table("analytics", "user_metrics", with_users=["alice", "bob"])
    """
    client = get_governance_client()
    # Get current user's username from environment variable
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = ShareRequest(path=table_path, users=with_users or [], groups=with_groups or [])
    response = share_data_sharing_share_post.sync(client=client, body=request)

    # Log warnings if there were errors
    if response.errors:
        logger = logging.getLogger(__name__)
        for error in response.errors:
            logger.warning(f"Error sharing table {namespace}.{table_name}: {error}")

    return response


def unshare_table(
    namespace: str,
    table_name: str,
    from_users: Optional[List[str]] = None,
    from_groups: Optional[List[str]] = None,
) -> UnshareResponse:
    """
    Remove sharing permissions from a SQL warehouse table.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")
        from_users: List of usernames to remove access from
        from_groups: List of group names to remove access from

    Returns:
        UnsharePathResponse with unsharing details and success/error information

    Example:
        unshare_table("analytics", "user_metrics", from_users=["alice"])
    """
    client = get_governance_client()
    # Get current user's username from environment variable
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = UnshareRequest(path=table_path, users=from_users or [], groups=from_groups or [])
    response = unshare_data_sharing_unshare_post.sync(client=client, body=request)

    # Log warnings if there were errors
    if response.errors:
        logger = logging.getLogger(__name__)
        for error in response.errors:
            logger.warning(f"Error unsharing table {namespace}.{table_name}: {error}")

    return response


def make_table_public(
    namespace: str,
    table_name: str,
) -> PublicAccessResponse:
    """
    Make a SQL warehouse table publicly accessible.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")

    Returns:
        PublicAccessResponse with path and is_public status

    Example:
        make_table_public("research", "public_dataset")
    """
    client = get_governance_client()
    # Get current user's username from environment variable
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = PathRequest(path=table_path)
    return make_path_public_sharing_make_public_post.sync(client=client, body=request)


def make_table_private(
    namespace: str,
    table_name: str,
) -> PublicAccessResponse:
    """
    Remove public access from a SQL warehouse table.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")

    Returns:
        PublicAccessResponse with path and is_public status (should be False)

    Example:
        make_table_private("research", "sensitive_data")
    """
    client = get_governance_client()
    # Get current user's username from environment variable
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = PathRequest(path=table_path)
    return make_path_private_sharing_make_private_post.sync(client=client, body=request)
