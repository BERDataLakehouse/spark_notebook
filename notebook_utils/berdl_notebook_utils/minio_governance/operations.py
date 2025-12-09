"""
Utility functions for BERDL MinIO Data Governance integration
"""

import fcntl
import json
import logging
import os
import time
from pathlib import Path
from typing import TypedDict

from governance_client.api.credentials import get_credentials_credentials_get
from governance_client.api.health import health_check_health_get
from governance_client.api.management import (
    add_group_member_management_groups_group_name_members_username_post,
    create_group_management_groups_group_name_post,
    list_groups_management_groups_get,
    list_users_management_users_get,
    remove_group_member_management_groups_group_name_members_username_delete,
)
from governance_client.api.sharing import (
    get_path_access_info_sharing_get_path_access_info_post,
    make_path_private_sharing_make_private_post,
    make_path_public_sharing_make_public_post,
    share_data_sharing_share_post,
    unshare_data_sharing_unshare_post,
)
from governance_client.api.workspaces import (
    get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get,
    get_my_accessible_paths_workspaces_me_accessible_paths_get,
    get_my_groups_workspaces_me_groups_get,
    get_my_policies_workspaces_me_policies_get,
    get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get,
    get_my_workspace_workspaces_me_get,
    get_namespace_prefix_workspaces_me_namespace_prefix_get,
)
from governance_client.models import (
    CredentialsResponse,
    ErrorResponse,
    GroupManagementResponse,
    HealthResponse,
    NamespacePrefixResponse,
    PathAccessResponse,
    PathRequest,
    PublicAccessResponse,
    ShareRequest,
    ShareResponse,
    UnshareRequest,
    UnshareResponse,
    UserAccessiblePathsResponse,
    UserGroupsResponse,
    UserPoliciesResponse,
    UserSqlWarehousePrefixResponse,
)
from governance_client.types import UNSET

from berdl_notebook_utils import get_settings
from berdl_notebook_utils.clients import get_governance_client

# =============================================================================
# TYPE DEFINITIONS
# =============================================================================


class TenantCreationResult(TypedDict):
    """Result of tenant creation and user assignment operation."""

    create_tenant: GroupManagementResponse | ErrorResponse
    add_members: list[tuple[str, GroupManagementResponse | ErrorResponse]]


# =============================================================================
# CONSTANTS
# =============================================================================

# Default BERDL storage configuration
SQL_WAREHOUSE_BUCKET = "cdm-lake"  # TODO: change to berdl-lake
SQL_USER_WAREHOUSE_PATH = "users-sql-warehouse"

# Credential caching configuration
CREDENTIALS_CACHE_FILE = ".berdl_minio_credentials"


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _get_credentials_cache_path() -> Path:
    """Get the path to the credentials cache file in the user's home directory."""
    return Path.home() / CREDENTIALS_CACHE_FILE


def _read_cached_credentials(cache_path: Path) -> CredentialsResponse | None:
    """Read credentials from cache file. Returns None if file doesn't exist or is corrupted."""
    try:
        if not cache_path.exists():
            return None
        with open(cache_path, "r") as f:
            data = json.load(f)
        return CredentialsResponse.from_dict(data)
    except (json.JSONDecodeError, TypeError, KeyError, OSError):
        return None


def _write_credentials_cache(cache_path: Path, credentials: CredentialsResponse) -> None:
    """Write credentials to cache file."""
    try:
        with open(cache_path, "w") as f:
            json.dump(credentials.to_dict(), f)
    except (OSError, TypeError):
        pass


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

    Uses file locking to prevent race conditions when multiple processes/notebooks
    try to access credentials simultaneously.

    Sets the following environment variables:
    - MINIO_ACCESS_KEY: User's MinIO access key
    - MINIO_SECRET_KEY: User's MinIO secret key

    Returns:
        CredentialsResponse with username, access_key, and secret_key
    """
    cache_path = _get_credentials_cache_path()
    lock_path = cache_path.with_suffix(".lock")

    # Use file locking to prevent concurrent access
    with open(lock_path, "w") as lock_file:
        try:
            # Acquire exclusive lock (blocks until available)
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

            # Try to load from cache first (double-check after acquiring lock)
            cached_credentials = _read_cached_credentials(cache_path)
            if cached_credentials:
                credentials = cached_credentials
            else:
                # No cache or cache corrupted, fetch fresh credentials
                client = get_governance_client()
                api_response = get_credentials_credentials_get.sync(client=client)
                if isinstance(api_response, CredentialsResponse):
                    credentials = api_response
                    _write_credentials_cache(cache_path, credentials)
                else:
                    raise RuntimeError("Failed to fetch credentials from API")
        finally:
            # Lock is automatically released when file is closed
            pass

    # Clean up lock file if it exists
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass

    # Set MinIO credentials as environment variables
    os.environ["MINIO_ACCESS_KEY"] = credentials.access_key
    os.environ["MINIO_SECRET_KEY"] = credentials.secret_key

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


def get_namespace_prefix(tenant: str | None = None) -> NamespacePrefixResponse:
    """
    Get governance namespace prefix for the current user or a specific tenant.

    Args:
        tenant: Optional tenant (group) name. If provided, returns tenant namespace prefix
               (requires group membership). If None, returns user namespace prefix.

    Returns:
        NamespacePrefixResponse with username, user_namespace_prefix, and optionally
        tenant and tenant_namespace_prefix if tenant is specified

    Example:
        # Get user namespace prefix
        response = get_namespace_prefix()
        user_prefix = response.user_namespace_prefix  # e.g., "u_tgu2__"

        # Get tenant namespace prefix
        response = get_namespace_prefix(tenant="research_team")
        tenant_prefix = response.tenant_namespace_prefix  # e.g., "t_research_team__"
    """

    client = get_governance_client()
    return get_namespace_prefix_workspaces_me_namespace_prefix_get.sync(
        client=client, tenant=tenant if tenant is not None else UNSET
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


def get_my_groups() -> UserGroupsResponse:
    """
    Get list of groups the current user belongs to.

    Returns:
        UserGroupsResponse with username, groups list, and group_count
    """
    client = get_governance_client()
    return get_my_groups_workspaces_me_groups_get.sync(client=client)


def get_my_accessible_paths() -> UserAccessiblePathsResponse:
    """
    Get all S3 paths accessible to the current user through their policies and group memberships.

    This includes paths from:
    - User's personal policies
    - Group policies
    - Shared paths

    Returns:
        UserAccessiblePathsResponse with username, accessible_paths list, and total_paths count
    """
    client = get_governance_client()
    return get_my_accessible_paths_workspaces_me_accessible_paths_get.sync(client=client)


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
    with_users: list[str] | None = None,
    with_groups: list[str] | None = None,
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
    request = ShareRequest(path=table_path, with_users=with_users or [], with_groups=with_groups or [])
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
    from_users: list[str] | None = None,
    from_groups: list[str] | None = None,
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
    request = UnshareRequest(path=table_path, from_users=from_users or [], from_groups=from_groups or [])
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


# =============================================================================
# MANAGEMENT OPERATIONS - Group and tenant management
# =============================================================================


def list_groups() -> dict | ErrorResponse | None:
    """
    List all groups in the system with membership information.

    Returns:
        Dictionary with group information including members, or ErrorResponse on failure.
        The dictionary contains group names as keys with their member lists.

    Example:
        groups = list_groups()
        # Returns: {'research_team': ['alice', 'bob'], 'data_engineers': ['charlie']}
    """
    client = get_governance_client()
    return list_groups_management_groups_get.sync(client=client)


def list_users():
    """
    List all users in the system.

    Returns:
        UserListResponse with user information, or ErrorResponse on failure.

    Example:
        users = list_users()
        # Returns list of user information
    """
    client = get_governance_client()
    return list_users_management_users_get.sync(client=client)


def add_group_member(
    group_name: str,
    usernames: list[str],
    read_only: bool = False,
) -> list[tuple[str, GroupManagementResponse | ErrorResponse | None]]:
    """
    Add users to an existing group.

    Args:
        group_name: Name of the group to add users to (without "ro" suffix)
        usernames: List of usernames to add as members
        read_only: If True, adds users to the read-only variant ({group_name}ro).
                   If False (default), adds to the read/write group.

    Returns:
        List of tuples (username, response) for each user addition attempt

    Examples:
        # Add to read/write group (default)
        results = add_group_member("kbase", ["alice", "bob"])

        # Add to read-only group
        results = add_group_member("kbase", ["charlie"], read_only=True)

        for username, response in results:
            if isinstance(response, GroupManagementResponse):
                print(f"Successfully added {username} to group")
    """
    client = get_governance_client()

    # Determine target group based on read_only flag
    target_group_name = f"{group_name}ro" if read_only else group_name

    results = []
    for username in usernames:
        time.sleep(1)
        response = add_group_member_management_groups_group_name_members_username_post.sync(
            client=client,
            group_name=target_group_name,
            username=username,
        )
        results.append((username, response))
    return results


def remove_group_member(
    group_name: str,
    usernames: list[str],
    read_only: bool = False,
) -> list[tuple[str, GroupManagementResponse | ErrorResponse | None]]:
    """
    Remove users from an existing group.

    Args:
        group_name: Name of the group to remove users from (without "ro" suffix)
        usernames: List of usernames to remove from the group
        read_only: If True, removes users from the read-only variant ({group_name}ro).
                   If False (default), removes from the read/write group.

    Returns:
        List of tuples (username, response) for each user removal attempt

    Examples:
        # Remove from read/write group (default)
        results = remove_group_member("kbase", ["alice", "bob"])

        # Remove from read-only group
        results = remove_group_member("kbase", ["charlie"], read_only=True)

        for username, response in results:
            if isinstance(response, GroupManagementResponse):
                print(f"Successfully removed {username} from group")
    """
    client = get_governance_client()

    # Determine target group based on read_only flag
    target_group_name = f"{group_name}ro" if read_only else group_name

    results = []
    for username in usernames:
        time.sleep(1)
        response = remove_group_member_management_groups_group_name_members_username_delete.sync(
            client=client,
            group_name=target_group_name,
            username=username,
        )
        results.append((username, response))
    return results


def create_tenant_and_assign_users(
    tenant_name: str,
    usernames: list[str] | None = None,
) -> TenantCreationResult:
    """
    Create a new tenant (group) and assign users to it.

    This is a convenience function that combines tenant creation and user assignment
    into a single operation. It creates a new group with shared workspace and policy
    configuration, then adds the specified users as members.

    Args:
        tenant_name: Name of the tenant/group to create
        usernames: List of usernames to add as members (optional)

    Returns:
        Dictionary with the following keys:
        - 'create_tenant': GroupManagementResponse or ErrorResponse from tenant creation
        - 'add_members': List of tuples (username, GroupManagementResponse or ErrorResponse)
                        for each user addition attempt
    """
    client = get_governance_client()
    logger = logging.getLogger(__name__)

    # Step 1: Create the tenant/group
    logger.info(f"Creating tenant: {tenant_name}")
    create_response = create_group_management_groups_group_name_post.sync(
        client=client,
        group_name=tenant_name,
    )

    result: TenantCreationResult = {
        "create_tenant": create_response,
        "add_members": [],
    }

    # Check if tenant creation was successful
    if isinstance(create_response, ErrorResponse):
        logger.error(f"Failed to create tenant {tenant_name}: {create_response.message}")
        return result

    # Step 2: Add users to the tenant if provided
    if usernames:
        logger.info(f"Adding {len(usernames)} users to tenant {tenant_name}")
        for username in usernames:
            time.sleep(1)
            try:
                add_response = add_group_member_management_groups_group_name_members_username_post.sync(
                    client=client,
                    group_name=tenant_name,
                    username=username,
                )
                result["add_members"].append((username, add_response))

                if isinstance(add_response, ErrorResponse):
                    logger.warning(f"Failed to add user {username} to tenant {tenant_name}: {add_response.message}")
                else:
                    logger.info(f"Successfully added user {username} to tenant {tenant_name}")

            except Exception as e:
                logger.error(f"Error adding user {username} to tenant {tenant_name}: {e}")
                error_response = ErrorResponse(
                    message=f"Exception occurred while adding {username}: {str(e)}", error_type="Exception"
                )
                result["add_members"].append((username, error_response))
                # Continue with other users even if one fails

    return result
