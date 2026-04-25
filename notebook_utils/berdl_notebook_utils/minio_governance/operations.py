"""
Utility functions for BERDL MinIO Data Governance integration
"""

import fcntl
import json
import logging
import os
import time
import warnings
from pathlib import Path
from collections.abc import Callable
from typing import TypeVar

from typing import TypedDict

import httpx
from governance_client.api.credentials import (
    get_credentials_credentials_get,
    rotate_credentials_credentials_rotate_post,
)
from governance_client.api.health import health_check_health_get
from governance_client.api.management import (
    add_group_member_management_groups_group_name_members_username_post,
    create_group_management_groups_group_name_post,
    list_groups_management_groups_get,
    list_users_management_users_get,
    regenerate_all_policies_management_migrate_regenerate_policies_post,
    remove_group_member_management_groups_group_name_members_username_delete,
)

# Polaris-specific imports — only available when governance client includes polaris endpoints
try:
    from governance_client.api.polaris import provision_polaris_user_polaris_user_provision_username_post
    from governance_client.api.management import (
        ensure_all_polaris_resources_management_migrate_ensure_polaris_resources_post,
    )
except ImportError:
    provision_polaris_user_polaris_user_provision_username_post = None  # type: ignore[assignment]
    ensure_all_polaris_resources_management_migrate_ensure_polaris_resources_post = None  # type: ignore[assignment]
from governance_client.api.management.list_group_names_management_groups_names_get import (
    sync as list_group_names_sync,
)
from governance_client.api.management.list_user_names_management_users_names_get import (
    sync as list_user_names_sync,
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
    RegeneratePoliciesResponse,
    ShareRequest,
    ShareResponse,
    UnshareRequest,
    UnshareResponse,
    UserAccessiblePathsResponse,
    UserGroupsResponse,
    UserNamesResponse,
    UserPoliciesResponse,
    UserSqlWarehousePrefixResponse,
)
from governance_client.types import UNSET

from .. import get_settings
from ..clients import get_governance_client
from ._cache import groups_cache, invalidate_all

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

# Credential caching configuration (Polaris only — MinIO uses direct API calls)
POLARIS_CREDENTIALS_CACHE_FILE = ".berdl_polaris_credentials"

_GROUPS_CACHE_KEY = "me"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

_T = TypeVar("_T")


def _fetch_with_file_cache(
    cache_path: Path,
    read_cache: Callable[[Path], _T | None],
    fetch: Callable[[], _T | None],
    write_cache: Callable[[Path, _T], None],
) -> _T | None:
    """Fetch credentials using file-based caching with exclusive file locking.

    The lock is released when the file handle is closed (exiting the `with` block).
    We intentionally do NOT delete the lock file afterward — another process
    may have already acquired a lock on it between our unlock and unlink.
    """
    lock_path = cache_path.with_suffix(".lock")
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

        cached = read_cache(cache_path)
        if cached is not None:
            return cached

        result = fetch()
        if result is not None:
            write_cache(cache_path, result)
        return result


def _get_polaris_cache_path() -> Path:
    """Get the path to the Polaris credentials cache file in the user's home directory."""
    return Path.home() / POLARIS_CREDENTIALS_CACHE_FILE


def _read_cached_polaris_credentials(cache_path: Path) -> "PolarisCredentials | None":
    """Read Polaris credentials from cache file. Returns None if file doesn't exist or is corrupted."""
    try:
        if not cache_path.exists():
            return None
        with open(cache_path, "r") as f:
            data = json.load(f)
        # Validate required keys are present
        if all(k in data for k in ("client_id", "client_secret", "personal_catalog", "tenant_catalogs")):
            return data
        return None
    except (json.JSONDecodeError, TypeError, KeyError, OSError):
        return None


def _write_polaris_credentials_cache(cache_path: Path, credentials: "PolarisCredentials") -> None:
    """Write Polaris credentials to cache file."""
    try:
        with open(cache_path, "w") as f:
            json.dump(credentials, f)
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
    response = health_check_health_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Health check failed: {response.message}")

    if not isinstance(response, HealthResponse):
        raise RuntimeError("Health check failed: no response from API")

    return response


def get_minio_credentials() -> CredentialsResponse:
    """
    Get MinIO credentials for the current user and set them as environment variables.

    Fetches credentials from the governance API (MMS). The API caches credentials
    server-side in PostgreSQL, so repeated calls are fast.

    Sets the following environment variables:
    - MINIO_ACCESS_KEY: User's MinIO access key
    - MINIO_SECRET_KEY: User's MinIO secret key

    Returns:
        CredentialsResponse with username, access_key, and secret_key
    """
    client = get_governance_client()
    api_response = get_credentials_credentials_get.sync(client=client)
    if not isinstance(api_response, CredentialsResponse):
        raise RuntimeError("Failed to fetch credentials from API")

    os.environ["MINIO_ACCESS_KEY"] = api_response.access_key
    os.environ["MINIO_SECRET_KEY"] = api_response.secret_key

    # Clear the cached settings so subsequent get_settings() calls pick up the
    # new MINIO_ACCESS_KEY / MINIO_SECRET_KEY env vars.
    get_settings.cache_clear()

    return api_response


class PolarisCredentials(TypedDict):
    """Polaris credential provisioning result."""

    client_id: str
    client_secret: str
    personal_catalog: str
    tenant_catalogs: list[str]


def _fetch_polaris_credentials() -> PolarisCredentials | None:
    """Fetch fresh Polaris credentials from the governance API."""
    settings = get_settings()
    polaris_logger = logging.getLogger(__name__)

    if provision_polaris_user_polaris_user_provision_username_post is None:
        polaris_logger.warning("Polaris API not available — governance client does not include polaris endpoints")
        return None

    client = get_governance_client()
    api_response = provision_polaris_user_polaris_user_provision_username_post.sync(
        username=settings.USER, client=client
    )

    if isinstance(api_response, ErrorResponse):
        polaris_logger.warning(f"Polaris provisioning failed: {api_response.message}")
        return None
    if api_response is None:
        polaris_logger.warning("Polaris provisioning returned no response")
        return None

    data = api_response.to_dict()
    return {
        "client_id": data.get("client_id", ""),
        "client_secret": data.get("client_secret", ""),
        "personal_catalog": data.get("personal_catalog", ""),
        "tenant_catalogs": data.get("tenant_catalogs", []),
    }


def get_polaris_credentials() -> PolarisCredentials | None:
    """
    Provision a Polaris catalog for the current user and set credentials as environment variables.

    Uses file locking and caching to prevent race conditions and avoid unnecessary
    API calls when credentials are already cached (same pattern as get_minio_credentials).

    Calls POST /polaris/user_provision/{username} on the governance API on cache miss.
    This provisions the user's Polaris environment (catalog, principal, roles, credentials,
    tenant access) and returns the configuration.

    Sets the following environment variables:
    - POLARIS_CREDENTIAL: client_id:client_secret for authenticating with Polaris
    - POLARIS_PERSONAL_CATALOG: Name of the user's personal Polaris catalog
    - POLARIS_TENANT_CATALOGS: Comma-separated list of tenant catalogs the user has access to

    Returns:
        PolarisCredentials dict, or None if Polaris is not configured
    """
    settings = get_settings()

    if not settings.POLARIS_CATALOG_URI:
        return None

    result = _fetch_with_file_cache(
        _get_polaris_cache_path(),
        _read_cached_polaris_credentials,
        _fetch_polaris_credentials,
        _write_polaris_credentials_cache,
    )
    if result is None:
        return None

    # Set as environment variables for Spark catalog configuration
    os.environ["POLARIS_CREDENTIAL"] = f"{result['client_id']}:{result['client_secret']}"
    os.environ["POLARIS_PERSONAL_CATALOG"] = result["personal_catalog"]
    os.environ["POLARIS_TENANT_CATALOGS"] = ",".join(result["tenant_catalogs"])

    return result


def rotate_minio_credentials() -> CredentialsResponse:
    """
    Rotate MinIO credentials for the current user and update environment variables.

    Calls POST /credentials/rotate to generate new credentials in MinIO,
    then updates the environment variables.

    Returns:
        CredentialsResponse with username, access_key, and secret_key
    """
    client = get_governance_client()
    api_response = rotate_credentials_credentials_rotate_post.sync(client=client)
    if not isinstance(api_response, CredentialsResponse):
        raise RuntimeError("Failed to rotate credentials from API")

    os.environ["MINIO_ACCESS_KEY"] = api_response.access_key
    os.environ["MINIO_SECRET_KEY"] = api_response.secret_key

    # Clear cached settings so downstream code sees fresh env vars
    get_settings.cache_clear()

    return api_response


def get_my_sql_warehouse() -> UserSqlWarehousePrefixResponse:
    """
    Get SQL warehouse prefix for the current user.

    Returns:
        UserSqlWarehousePrefixResponse with username and sql_warehouse_prefix
    """
    client = get_governance_client()
    response = get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get SQL warehouse prefix: {response.message}")

    if not isinstance(response, UserSqlWarehousePrefixResponse):
        raise RuntimeError("Failed to get SQL warehouse prefix: no response from API")

    return response


def get_group_sql_warehouse(group_name: str):
    """
    Get SQL warehouse prefix for a specific group.

    Args:
        group_name: Name of the group

    Returns:
        GroupSqlWarehousePrefixResponse with group_name and sql_warehouse_prefix
    """
    client = get_governance_client()
    response = get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get.sync(
        client=client, group_name=group_name
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get group SQL warehouse prefix: {response.message}")

    if response is None:
        raise RuntimeError("Failed to get group SQL warehouse prefix: no response from API")

    return response


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
        tenant_prefix = response.tenant_namespace_prefix  # e.g., "research_team_"
    """

    client = get_governance_client()
    response = get_namespace_prefix_workspaces_me_namespace_prefix_get.sync(
        client=client, tenant=tenant if tenant is not None else UNSET
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get namespace prefix: {response.message}")

    if not isinstance(response, NamespacePrefixResponse):
        raise RuntimeError("Failed to get namespace prefix: no response from API")

    return response


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
    response = get_my_policies_workspaces_me_policies_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get policies: {response.message}")

    if not isinstance(response, UserPoliciesResponse):
        raise RuntimeError("Failed to get policies: no response from API")

    return response


def get_my_groups(force_refresh: bool = False) -> UserGroupsResponse:
    """
    Get list of groups the current user belongs to.

    Results are cached in-process for 3600s. Mutations in this package that
    can change the current user's group membership (add/remove_tenant_member,
    add/remove_group_member, create_tenant_and_assign_users) bust the cache.

    Args:
        force_refresh: If True, bypass the cache and hit the governance API.

    Returns:
        UserGroupsResponse with username, groups list, and group_count
    """
    if not force_refresh:
        cached = groups_cache.get(_GROUPS_CACHE_KEY)
        if cached is not None:
            return cached

    client = get_governance_client()
    response = get_my_groups_workspaces_me_groups_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get groups: {response.message}")

    if not isinstance(response, UserGroupsResponse):
        raise RuntimeError("Failed to get groups: no response from API")

    groups_cache.set(_GROUPS_CACHE_KEY, response)
    return response


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
    response = get_my_accessible_paths_workspaces_me_accessible_paths_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get accessible paths: {response.message}")

    if not isinstance(response, UserAccessiblePathsResponse):
        raise RuntimeError("Failed to get accessible paths: no response from API")

    return response


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
    response = get_path_access_info_sharing_get_path_access_info_post.sync(client=client, body=request)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get table access info: {response.message}")

    if not isinstance(response, PathAccessResponse):
        raise RuntimeError("Failed to get table access info: no response from API")

    return response


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
    warnings.warn(
        "share_table is deprecated and will be removed in a future release. "
        "Direct path sharing is no longer recommended. Please create a Tenant Workspace "
        "and request access to the tenant for sharing activities.",
        DeprecationWarning,
        stacklevel=2,
    )
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
    warnings.warn(
        "unshare_table is deprecated and will be removed in a future release. "
        "Direct path sharing is no longer recommended. Please create a Tenant Workspace "
        "and request access to the tenant for unsharing activities.",
        DeprecationWarning,
        stacklevel=2,
    )
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

    .. deprecated::
        ``make_table_public`` is deprecated and will be removed in a future release.
        Direct public path sharing is no longer recommended. Please add a namespace
        under the ``globalusers`` tenant to grant public access.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")

    Returns:
        PublicAccessResponse with path and is_public status

    Example:
        make_table_public("research", "public_dataset")
    """
    warnings.warn(
        "make_table_public is deprecated and will be removed in a future release. "
        "Direct public path sharing is no longer recommended. Please add a namespace "
        "under the `globalusers` tenant to grant public access.",
        DeprecationWarning,
        stacklevel=2,
    )
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

    .. deprecated::
        ``make_table_private`` is deprecated and will be removed in a future release.
        Direct public path sharing is no longer recommended. Please remove the namespace
        under the ``globalusers`` tenant to revoke public access.

    Args:
        namespace: Database namespace (e.g., "test" or "test.db")
        table_name: Table name (e.g., "test_employees")

    Returns:
        PublicAccessResponse with path and is_public status (should be False)

    Example:
        make_table_private("research", "sensitive_data")
    """
    warnings.warn(
        "make_table_private is deprecated and will be removed in a future release. "
        "Direct public path sharing is no longer recommended. Please remove the namespace "
        "under the `globalusers` tenant to revoke public access.",
        DeprecationWarning,
        stacklevel=2,
    )
    client = get_governance_client()
    # Get current user's username from environment variable
    username = get_settings().USER

    table_path = _build_table_path(username, namespace, table_name)
    request = PathRequest(path=table_path)
    return make_path_private_sharing_make_private_post.sync(client=client, body=request)


# =============================================================================
# MANAGEMENT OPERATIONS - Group and tenant management
# =============================================================================


def list_available_groups() -> list[str]:
    """
    List all available tenant group names that users can request access to.

    This endpoint is available to any authenticated user (not admin-only).
    Returns only the base tenant names, filtering out read-only variants
    (groups ending with "ro").

    Returns:
        List of tenant group names (e.g., ["kbase", "research", "shared-data"])

    Example:
        # List all available groups to request access to
        groups = list_available_groups()
        print(f"Available groups: {groups}")
        # Output: Available groups: ['kbase', 'research', 'shared-data']

        # Then request access to one of them
        request_tenant_access("kbase", permission="read_only")
    """
    client = get_governance_client()
    response = list_group_names_sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to list groups: {response.message}")

    if response is None:
        raise RuntimeError("Failed to list groups: no response from API")

    # Filter out read-only groups (ending with "ro") and return only base tenant names
    return [name for name in response.group_names if not name.endswith("ro")]


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


def list_users(page: int = 1, page_size: int = 500):
    """
    List all users in the system with full details.

    This fetches full user info (policies, groups, paths) for each user,
    which can be slow with many users. If you only need usernames,
    use ``list_user_names()`` instead.

    Args:
        page: Page number (1-based). Default: 1.
        page_size: Number of users per page. Default: 500.

    Returns:
        UserListResponse with user information, or ErrorResponse on failure.

    Example:
        users = list_users()
        # Returns list of user information
    """
    client = get_governance_client()
    return list_users_management_users_get.sync(client=client, page=page, page_size=page_size)


def list_user_names() -> list[str]:
    """
    List all usernames in the system (lightweight).

    This is much faster than ``list_users()`` because it only returns
    usernames without fetching full user details (policies, groups, paths).

    Returns:
        List of usernames.

    Raises:
        RuntimeError: If the API call fails.
    """
    client = get_governance_client()
    response = list_user_names_sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to list usernames: {response.message}")

    if not isinstance(response, UserNamesResponse):
        raise RuntimeError("Failed to list usernames: no response from API")

    return response.usernames


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
    invalidate_all()
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
    invalidate_all()
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

    invalidate_all()
    return result


# =============================================================================
# TENANT ACCESS REQUEST - Self-service access requests via Slack approval
# =============================================================================


class TenantAccessRequestResponse(TypedDict):
    """Response from submitting a tenant access request."""

    status: str
    message: str
    requester: str
    tenant_name: str
    permission: str


def request_tenant_access(
    tenant_name: str,
    permission: str = "read_only",
    justification: str | None = None,
) -> TenantAccessRequestResponse:
    """
    Request access to a tenant group via Slack approval workflow.

    This submits an access request that will be sent to a Slack channel
    configured by the SLACK_CHANNEL_ID environment variable. Admins with
    CDM_JUPYTERHUB_ADMIN role can approve or deny the request with interactive
    buttons.

    Args:
        tenant_name: Name of the tenant to request access to (e.g., "kbase")
        permission: Level of access - "read_only" (default) or "read_write"
        justification: Optional reason for requesting access (recommended)

    Returns:
        TenantAccessRequestResponse with status and confirmation message

    Raises:
        ValueError: If TENANT_ACCESS_SERVICE_URL is not configured
        RuntimeError: If the request fails

    Example:
        # Request read-only access
        result = request_tenant_access("kbase")
        print(f"Request submitted: {result['status']}")

        # Request read-write access with justification
        result = request_tenant_access(
            "research-data",
            permission="read_write",
            justification="Need write access for data pipeline project"
        )
    """

    settings = get_settings()

    if not settings.TENANT_ACCESS_SERVICE_URL:
        raise ValueError(
            "TENANT_ACCESS_SERVICE_URL is not configured. "
            "Contact your administrator to enable self-service tenant access requests."
        )

    url = f"{str(settings.TENANT_ACCESS_SERVICE_URL).rstrip('/')}/requests/"

    payload = {
        "tenant_name": tenant_name,
        "permission": permission,
    }
    if justification:
        payload["justification"] = justification

    try:
        response = httpx.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {settings.KBASE_AUTH_TOKEN}"},
            timeout=30.0,
        )
        response.raise_for_status()
        data = response.json()

        logger = logging.getLogger(__name__)
        logger.info(f"Submitted tenant access request: {tenant_name} ({permission}) - {data['status']}")

        return TenantAccessRequestResponse(
            status=data["status"],
            message=data["message"],
            requester=data["requester"],
            tenant_name=data["tenant_name"],
            permission=data["permission"],
        )

    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Failed to submit access request: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        raise RuntimeError(f"Failed to connect to tenant access service: {e}")


# =============================================================================
# MIGRATION - Admin-only bulk operations for IAM + Polaris migration
# =============================================================================


def regenerate_policies() -> RegeneratePoliciesResponse:
    """
    Force-regenerate all MinIO IAM HOME policies from the current template.

    This admin-only endpoint updates pre-existing policies to include new path
    statements (e.g., Iceberg paths). Each regeneration is independent — errors
    do not block others.

    Returns:
        RegeneratePoliciesResponse with users_updated, groups_updated, errors,
        performed_by, and timestamp

    Raises:
        RuntimeError: If the request fails (e.g., insufficient permissions)

    Example:
        result = regenerate_policies()
        print(f"Updated {result.users_updated} users, {result.groups_updated} groups")
    """
    client = get_governance_client()
    response = regenerate_all_policies_management_migrate_regenerate_policies_post.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to regenerate policies: {response.message}")

    if response is None:
        raise RuntimeError("Failed to regenerate policies: no response from API")

    return response


def ensure_polaris_resources():
    """
    Ensure Polaris resources exist for all users and groups.

    Creates Polaris principals, personal catalogs, and roles for all users.
    Creates tenant catalogs for all base groups. Grants correct principal roles
    based on group memberships. All operations are idempotent.

    Returns:
        EnsurePolarisResponse with users_provisioned, groups_provisioned, errors,
        or ErrorResponse on failure.
    """
    if ensure_all_polaris_resources_management_migrate_ensure_polaris_resources_post is None:
        raise RuntimeError("Polaris API not available — governance client does not include polaris endpoints")

    client = get_governance_client()
    return ensure_all_polaris_resources_management_migrate_ensure_polaris_resources_post.sync(client=client)
