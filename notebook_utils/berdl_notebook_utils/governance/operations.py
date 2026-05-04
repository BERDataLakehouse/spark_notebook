"""
Utility functions for BERDL Data Governance integration.

Covers the unified S3 + Polaris credential bundle (``/credentials/``,
``/credentials/rotate``) and read-only Polaris catalog metadata
discovery (``/polaris/effective-access/me``). Explicit Polaris
provisioning / Polaris-only rotation are still available as recovery
operations but are no longer part of the day-to-day flow.
"""

import logging
import os
import time
import warnings
from collections.abc import Sequence
from typing import Any, TypedDict

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

# Polaris-specific imports — only available when governance client includes Polaris endpoints.
try:
    from governance_client.api.polaris import provision_polaris_user_polaris_user_provision_username_post
except ImportError:
    provision_polaris_user_polaris_user_provision_username_post = None  # type: ignore[assignment]

try:
    from governance_client.api.polaris import rotate_polaris_credentials_polaris_credentials_rotate_username_post
except ImportError:
    rotate_polaris_credentials_polaris_credentials_rotate_username_post = None  # type: ignore[assignment]

# /polaris/effective-access/me — read-only catalog metadata, no provisioning side effect.
try:
    from governance_client.api.polaris import get_my_effective_access_polaris_effective_access_me_get
except ImportError:
    get_my_effective_access_polaris_effective_access_me_get = None  # type: ignore[assignment]

try:
    from governance_client.api.management import (
        ensure_all_polaris_resources_management_migrate_ensure_polaris_resources_post,
    )
except ImportError:
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
    PolarisEffectiveAccessResponse,
    GroupManagementResponse,
    HealthResponse,
    NamespacePrefixResponse,
    PathAccessResponse,
    PathRequest,
    PublicAccessResponse,
    RegeneratePoliciesRequest,
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

_GROUPS_CACHE_KEY = "me"


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


def _set_credential_env(api_response: CredentialsResponse) -> None:
    """Populate S3 + Polaris credential env vars from a unified response.

    The unified ``/credentials/`` and ``/credentials/rotate`` endpoints
    return both backends in one call. Setting both here means downstream
    code (Spark/Trino/Iceberg config builders) can read from a single,
    consistent source — and one MMS roundtrip is enough to refresh
    everything credential-related.
    """
    os.environ["S3_ACCESS_KEY"] = api_response.s3_access_key
    os.environ["S3_SECRET_KEY"] = api_response.s3_secret_key
    os.environ["POLARIS_CREDENTIAL"] = f"{api_response.polaris_client_id}:{api_response.polaris_client_secret}"
    # Clear the cached settings so subsequent get_settings() calls pick up the
    # new S3_ACCESS_KEY / S3_SECRET_KEY / POLARIS_CREDENTIAL env vars.
    get_settings.cache_clear()


def get_credentials() -> CredentialsResponse:
    """Get unified S3 + Polaris credentials for the current user.

    Calls ``GET /credentials/``. The API caches the bundle server-side
    in PostgreSQL, so repeated calls are fast (cache-first). On first
    call MMS self-bootstraps both the MinIO/S3 user AND the Polaris
    principal + personal catalog + role bindings.

    Sets the following environment variables in one shot:
    - ``S3_ACCESS_KEY``: User's S3 IAM access key
    - ``S3_SECRET_KEY``: User's S3 IAM secret key
    - ``POLARIS_CREDENTIAL``: ``client_id:client_secret`` for Polaris OAuth

    For catalog metadata (``POLARIS_PERSONAL_CATALOG`` /
    ``POLARIS_TENANT_CATALOGS``) call :func:`get_polaris_catalog_info`.

    Returns:
        CredentialsResponse with username, ``s3_access_key``,
        ``s3_secret_key``, ``polaris_client_id``, ``polaris_client_secret``.
    """
    client = get_governance_client()
    api_response = get_credentials_credentials_get.sync(client=client)
    if not isinstance(api_response, CredentialsResponse):
        raise RuntimeError("Failed to fetch credentials from API")

    _set_credential_env(api_response)
    return api_response


class PolarisCatalogInfo(TypedDict):
    """Polaris catalog discovery result.

    Returned by :func:`get_polaris_catalog_info`. Credentials are NOT
    included — fetch those via :func:`get_credentials` (the unified
    bundle includes the Polaris OAuth half).
    """

    personal_catalog: str
    tenant_catalogs: list[str]


class PolarisProvisioningResult(TypedDict):
    """Polaris explicit-provisioning result.

    Returned by :func:`provision_polaris_user` and
    :func:`rotate_polaris_credentials` — both call Polaris-specific
    endpoints that include credentials AND catalog metadata.
    """

    client_id: str
    client_secret: str
    personal_catalog: str
    tenant_catalogs: list[str]


def _set_polaris_full_env(creds: PolarisProvisioningResult) -> None:
    """Set all four POLARIS_* env vars from a provisioning response."""
    os.environ["POLARIS_CREDENTIAL"] = f"{creds['client_id']}:{creds['client_secret']}"
    os.environ["POLARIS_PERSONAL_CATALOG"] = creds["personal_catalog"]
    os.environ["POLARIS_TENANT_CATALOGS"] = ",".join(creds["tenant_catalogs"])
    get_settings.cache_clear()


def _set_polaris_catalog_env(info: PolarisCatalogInfo) -> None:
    """Set the catalog-metadata POLARIS_* env vars (no credential touch)."""
    os.environ["POLARIS_PERSONAL_CATALOG"] = info["personal_catalog"]
    os.environ["POLARIS_TENANT_CATALOGS"] = ",".join(info["tenant_catalogs"])
    get_settings.cache_clear()


def _parse_polaris_provisioning_response(api_response: Any, action: str) -> PolarisProvisioningResult | None:
    """Convert a Polaris provisioning/rotation response to the notebook shape."""
    polaris_logger = logging.getLogger(__name__)

    if isinstance(api_response, ErrorResponse):
        polaris_logger.warning("Polaris %s failed: %s", action, api_response.message)
        return None
    if api_response is None:
        polaris_logger.warning("Polaris %s returned no response", action)
        return None

    data = api_response.to_dict()
    return {
        "client_id": data.get("client_id", ""),
        "client_secret": data.get("client_secret", ""),
        "personal_catalog": data.get("personal_catalog", ""),
        "tenant_catalogs": data.get("tenant_catalogs", []),
    }


def get_polaris_catalog_info() -> PolarisCatalogInfo | None:
    """Fetch personal + tenant catalog names for the current user.

    Calls ``GET /polaris/effective-access/me`` — a read-only discovery
    endpoint that does NOT trigger any provisioning. For the user's
    Polaris OAuth credentials, call :func:`get_credentials` (the
    unified bundle).

    Sets:
    - ``POLARIS_PERSONAL_CATALOG``: e.g. ``user_alice``
    - ``POLARIS_TENANT_CATALOGS``: comma-separated tenant catalog names

    Returns:
        PolarisCatalogInfo dict, or None if Polaris is not configured
        or the installed governance client doesn't include the
        effective-access endpoint.
    """
    settings = get_settings()
    polaris_logger = logging.getLogger(__name__)

    if not settings.POLARIS_CATALOG_URI:
        return None

    if get_my_effective_access_polaris_effective_access_me_get is None:
        polaris_logger.warning("Polaris effective-access API not available — governance client is out of date")
        return None

    client = get_governance_client()
    api_response = get_my_effective_access_polaris_effective_access_me_get.sync(client=client)

    if isinstance(api_response, ErrorResponse):
        polaris_logger.warning("Polaris effective-access fetch failed: %s", api_response.message)
        return None
    if not isinstance(api_response, PolarisEffectiveAccessResponse):
        polaris_logger.warning("Polaris effective-access returned no response")
        return None

    info: PolarisCatalogInfo = {
        "personal_catalog": api_response.personal_catalog,
        "tenant_catalogs": [t.catalog_name for t in api_response.group_tenants],
    }
    _set_polaris_catalog_env(info)
    return info


def provision_polaris_user() -> PolarisProvisioningResult | None:
    """Explicitly (re-)provision the user's Polaris environment.

    Calls ``POST /polaris/user_provision/{username}``. This is an
    **explicit recovery / admin** operation — day-to-day flows should
    use :func:`get_credentials` (which self-bootstraps the Polaris
    principal on cache miss) plus :func:`get_polaris_catalog_info`
    for the catalog metadata.

    Use this when:
    - The Polaris principal/catalog state needs to be re-asserted from
      MMS (e.g. after manual cleanup or schema drift).
    - You want a single response that includes both credentials and
      catalog metadata in one call.

    Sets all POLARIS_* env vars (POLARIS_CREDENTIAL,
    POLARIS_PERSONAL_CATALOG, POLARIS_TENANT_CATALOGS).

    Returns:
        PolarisProvisioningResult dict, or None if Polaris is not
        configured.
    """
    settings = get_settings()
    polaris_logger = logging.getLogger(__name__)

    if not settings.POLARIS_CATALOG_URI:
        return None

    if provision_polaris_user_polaris_user_provision_username_post is None:
        polaris_logger.warning("Polaris provisioning API not available — governance client is out of date")
        return None

    client = get_governance_client()
    api_response = provision_polaris_user_polaris_user_provision_username_post.sync(
        username=settings.USER, client=client
    )

    result = _parse_polaris_provisioning_response(api_response, "provisioning")
    if result is None:
        return None

    _set_polaris_full_env(result)
    return result


def rotate_polaris_credentials() -> PolarisProvisioningResult | None:
    """Polaris-only credential rotation (S3 IAM credentials untouched).

    The unified :func:`rotate_credentials` rotates BOTH backends — use
    that for routine rotation. This function is for the narrow case
    where ONLY the Polaris OAuth secret is suspected of compromise
    and you want to leave the S3 IAM secret alone.

    Restart any long-lived engines that cached the Polaris secret
    (Spark Connect, Trino sessions) after calling.

    Returns:
        PolarisProvisioningResult dict, or None if Polaris is not
        configured or the installed governance client lacks the
        Polaris rotate endpoint.
    """
    settings = get_settings()
    polaris_logger = logging.getLogger(__name__)

    if not settings.POLARIS_CATALOG_URI:
        return None

    if rotate_polaris_credentials_polaris_credentials_rotate_username_post is None:
        polaris_logger.warning("Polaris rotate API not available — governance client is out of date")
        return None

    client = get_governance_client()
    api_response = rotate_polaris_credentials_polaris_credentials_rotate_username_post.sync(
        username=settings.USER, client=client
    )

    result = _parse_polaris_provisioning_response(api_response, "credential rotation")
    if result is None:
        return None

    _set_polaris_full_env(result)
    return result


def _normalize_namespace_parts(namespace: str | Sequence[str]) -> list[str]:
    """Normalize a dotted namespace string or explicit namespace parts."""
    if isinstance(namespace, str):
        parts = namespace.split(".")
    else:
        parts = list(namespace)
    normalized = [part.strip() for part in parts]
    if not normalized or any(not part for part in normalized):
        raise ValueError("namespace must not be empty or contain empty parts")
    return normalized


def _governance_api_request(
    method: str,
    path: str,
    action: str,
    *,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
) -> Any:
    """Call a governance API endpoint not yet guaranteed in the generated client."""
    settings = get_settings()
    base_url = str(settings.GOVERNANCE_API_URL).rstrip("/")
    url = f"{base_url}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {settings.KBASE_AUTH_TOKEN}",
        "Content-Type": "application/json",
    }
    try:
        response = httpx.request(
            method,
            url,
            headers=headers,
            json=json_body,
            params=params,
            timeout=30.0,
        )
    except httpx.RequestError as exc:
        raise RuntimeError(f"Failed to connect to governance API while trying to {action}: {exc}") from exc

    if response.status_code >= 400:
        try:
            body = response.json()
        except ValueError:
            body = response.text
        if isinstance(body, dict):
            detail = body.get("detail") or body.get("message") or body
        else:
            detail = body
        raise RuntimeError(f"Failed to {action}: {response.status_code} {detail}")

    if not response.text:
        return None
    return response.json()


def _print_namespace_acl_refresh_hint(
    username: str,
    tenant_name: str,
    namespace_parts: Sequence[str],
    action: str,
) -> None:
    """Print the Spark refresh instruction expected by notebook users."""
    settings = get_settings()
    qualified_namespace = f"{tenant_name}.{'.'.join(namespace_parts)}"
    if username == settings.USER:
        target = "Run"
    else:
        target = f"Ask {username} to run"

    if action == "grant":
        print(
            f"Namespace access updated for {username}: {qualified_namespace}. "
            f"{target} refresh_spark_environment() before using {qualified_namespace} "
            "from Spark Connect or an existing Spark session."
        )
    else:
        print(
            f"Namespace access revoked for {username}: {qualified_namespace}. "
            f"{target} refresh_spark_environment() or restart Spark to clear stale catalog access."
        )


def grant_namespace_access(
    tenant_name: str,
    username: str,
    namespace: str | Sequence[str],
    access_level: str = "read",
    show_refresh_hint: bool = True,
) -> dict[str, Any]:
    """
    Grant a user read or write access to a Polaris namespace in a tenant catalog.

    Args:
        tenant_name: Base tenant name, not the ``ro`` group variant.
        username: KBase username receiving namespace access.
        namespace: Dotted namespace string (``"shared_data"`` or ``"geo.curated"``)
            or explicit namespace parts.
        access_level: ``"read"`` or ``"write"``.
        show_refresh_hint: If True, print the Spark refresh instruction after success.

    Returns:
        Namespace ACL grant response as a dictionary.
    """
    if access_level not in {"read", "write"}:
        raise ValueError("access_level must be 'read' or 'write'")

    namespace_parts = _normalize_namespace_parts(namespace)
    result = _governance_api_request(
        "POST",
        f"/tenants/{tenant_name}/namespace-acls",
        "grant namespace access",
        json_body={
            "username": username,
            "namespace": namespace_parts,
            "access_level": access_level,
        },
    )
    if show_refresh_hint:
        _print_namespace_acl_refresh_hint(username, tenant_name, namespace_parts, "grant")
    return result


def revoke_namespace_access(
    tenant_name: str,
    username: str,
    namespace: str | Sequence[str],
    show_refresh_hint: bool = True,
) -> dict[str, Any]:
    """
    Revoke a user's namespace ACL grant.

    Args:
        tenant_name: Base tenant name, not the ``ro`` group variant.
        username: KBase username whose namespace access should be revoked.
        namespace: Dotted namespace string or explicit namespace parts.
        show_refresh_hint: If True, print the Spark refresh instruction after success.

    Returns:
        Revoked namespace ACL grant response as a dictionary.
    """
    namespace_parts = _normalize_namespace_parts(namespace)
    result = _governance_api_request(
        "DELETE",
        f"/tenants/{tenant_name}/namespace-acls",
        "revoke namespace access",
        json_body={
            "username": username,
            "namespace": namespace_parts,
        },
    )
    if show_refresh_hint:
        _print_namespace_acl_refresh_hint(username, tenant_name, namespace_parts, "revoke")
    return result


def list_namespace_access(
    tenant_name: str | None = None,
    namespace: str | Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    """
    List namespace ACL grants.

    With ``tenant_name`` provided, this lists grants for a tenant and requires
    steward/admin access. Without ``tenant_name``, this lists grants for the
    authenticated user via ``GET /me/namespace-acls``.

    Args:
        tenant_name: Optional tenant name to inspect as steward/admin.
        namespace: Optional namespace filter for tenant-scoped listing.

    Returns:
        List of namespace ACL grant response dictionaries.
    """
    if tenant_name is None:
        if namespace is not None:
            raise ValueError("namespace filter requires tenant_name")
        return _governance_api_request(
            "GET",
            "/me/namespace-acls",
            "list current user's namespace access",
        )

    params = None
    if namespace is not None:
        params = {"namespace": ".".join(_normalize_namespace_parts(namespace))}

    return _governance_api_request(
        "GET",
        f"/tenants/{tenant_name}/namespace-acls",
        "list tenant namespace access",
        params=params,
    )


def rotate_credentials() -> CredentialsResponse:
    """Rotate BOTH S3 IAM and Polaris OAuth credentials in one call.

    Calls ``POST /credentials/rotate``. This is the canonical "respond
    to suspected credential compromise" primitive — rotating both
    backends together prevents long-lived sessions from continuing to
    authenticate with the old material.

    Updates the same env vars as :func:`get_credentials`:
    - ``S3_ACCESS_KEY`` / ``S3_SECRET_KEY`` (new IAM secret)
    - ``POLARIS_CREDENTIAL`` (new Polaris client_secret)

    Catalog metadata env vars (``POLARIS_PERSONAL_CATALOG`` /
    ``POLARIS_TENANT_CATALOGS``) are not touched here — call
    :func:`get_polaris_catalog_info` if you need to refresh them.

    For Polaris-only rotation use :func:`rotate_polaris_credentials`.

    Returns:
        CredentialsResponse with the rotated unified bundle.
    """
    client = get_governance_client()
    api_response = rotate_credentials_credentials_rotate_post.sync(client=client)
    if not isinstance(api_response, CredentialsResponse):
        raise RuntimeError("Failed to rotate credentials from API")

    _set_credential_env(api_response)
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


def regenerate_policies(
    exclude_users: list[str] | None = None,
    exclude_groups: list[str] | None = None,
) -> RegeneratePoliciesResponse:
    """
    Force-regenerate all MinIO IAM HOME policies from the current template.

    This admin-only endpoint updates pre-existing policies to include new path
    statements (e.g., Iceberg paths). Each regeneration is independent — errors
    do not block others.

    Args:
        exclude_users: Usernames to skip (e.g., system/service accounts such as
            ``hive``, ``mms``, ``cdm_task_service``). Names that do not exist
            are silently ignored. ``None`` or ``[]`` means "regenerate for all
            users". The caller is responsible for deciding which accounts are
            "system" — this function does not infer it.
        exclude_groups: Base group names to skip. The corresponding RO group
            (name + ``"ro"``) is also skipped automatically. Names that do not
            exist are silently ignored. ``None`` or ``[]`` means "regenerate
            for all groups".

    Returns:
        RegeneratePoliciesResponse with users_updated, groups_updated,
        users_skipped, groups_skipped, errors, performed_by, and timestamp.

    Raises:
        RuntimeError: If the request fails (e.g., insufficient permissions).

    Example:
        # Regenerate everything
        result = regenerate_policies()

        # Skip caller-defined system accounts
        SYSTEM_USERS = ["hive", "mms", "cdm_task_service"]
        result = regenerate_policies(exclude_users=SYSTEM_USERS)
        print(f"Updated {result.users_updated} users, "
              f"skipped {result.users_skipped}")
    """
    body = RegeneratePoliciesRequest(
        exclude_users=list(exclude_users) if exclude_users else [],
        exclude_groups=list(exclude_groups) if exclude_groups else [],
    )

    client = get_governance_client()
    response = regenerate_all_policies_management_migrate_regenerate_policies_post.sync(
        client=client,
        body=body,
    )

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
