"""
Tenant management functions for BERDL data stewards and admins.

This module wraps the /tenants API endpoints, providing steward-scoped
operations for managing tenant membership, metadata, and profiles.

For admin-only group-level operations (create/delete groups, bulk policy
regeneration), use the existing functions in operations.py instead.
"""

import logging

from governance_client.api.tenants import (
    add_tenant_member_tenants_tenant_name_members_username_post,
    assign_steward_tenants_tenant_name_stewards_username_post,
    get_tenant_detail_tenants_tenant_name_get,
    get_tenant_members_tenants_tenant_name_members_get,
    get_tenant_stewards_tenants_tenant_name_stewards_get,
    list_tenants_tenants_get,
    remove_steward_tenants_tenant_name_stewards_username_delete,
    remove_tenant_member_tenants_tenant_name_members_username_delete,
    update_tenant_metadata_tenants_tenant_name_patch,
)
from governance_client.models import (
    AddTenantMemberTenantsTenantNameMembersUsernamePostPermission,
    ErrorResponse,
    TenantDetailResponse,
    TenantMemberResponse,
    TenantMetadataResponse,
    TenantMetadataUpdate,
    TenantStewardResponse,
    TenantSummaryResponse,
)
from governance_client.types import UNSET

from ..clients import get_governance_client

logger = logging.getLogger(__name__)


def _error_message(response: ErrorResponse) -> str:
    """Extract a human-readable message from an ErrorResponse, handling Unset fields."""
    msg = response.message
    if msg is not UNSET and msg is not None:
        return str(msg)
    # FastAPI's HTTPException returns {"detail": "..."} which lands in additional_properties
    detail = response.additional_properties.get("detail")
    if detail:
        return str(detail)
    # Fall back to error_type or the full dict
    if response.error_type is not UNSET and response.error_type is not None:
        return f"{response.error_type} (code={response.error})"
    return str(response.to_dict())


# Map user-friendly permission strings to the generated enum
_PERMISSION_MAP = {
    "read_write": AddTenantMemberTenantsTenantNameMembersUsernamePostPermission.READ_WRITE,
    "read_only": AddTenantMemberTenantsTenantNameMembersUsernamePostPermission.READ_ONLY,
}


def list_tenants() -> list[TenantSummaryResponse]:
    """
    List all tenants with summary info.

    Returns a list of tenants with member count, and whether the current user
    is a member or steward of each tenant.

    Returns:
        List of TenantSummaryResponse objects

    Example:
        tenants = list_tenants()
        for t in tenants:
            print(f"{t.tenant_name} - {t.member_count} members")
    """
    client = get_governance_client()
    response = list_tenants_tenants_get.sync(client=client)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to list tenants: {_error_message(response)}")

    if not isinstance(response, list):
        raise RuntimeError("Failed to list tenants: no response from API")

    return response


def get_my_steward_tenants() -> list[TenantSummaryResponse]:
    """
    List tenants where the current user is a steward.

    Convenience wrapper around list_tenants() that filters to only
    tenants where is_steward is True.

    Returns:
        List of TenantSummaryResponse objects where the user is a steward

    Example:
        my_tenants = get_my_steward_tenants()
        for t in my_tenants:
            print(f"I steward: {t.tenant_name} ({t.member_count} members)")
    """
    return [t for t in list_tenants() if t.is_steward]


def get_tenant_detail(tenant_name: str) -> TenantDetailResponse:
    """
    Get full tenant detail: metadata, stewards, members with profiles, and storage paths.

    Requires the current user to be a member, steward, or admin.

    Args:
        tenant_name: Name of the tenant (MinIO group name)

    Returns:
        TenantDetailResponse with metadata, stewards, members, member_count,
        and storage_paths

    Example:
        detail = get_tenant_detail("kbase")
        print(f"{detail.metadata.display_name} - {detail.member_count} members")
        for m in detail.members:
            print(f"  {m.username} ({m.display_name}) - {m.access_level}")
    """
    client = get_governance_client()
    response = get_tenant_detail_tenants_tenant_name_get.sync(client=client, tenant_name=tenant_name)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get tenant detail: {_error_message(response)}")

    if not isinstance(response, TenantDetailResponse):
        raise RuntimeError("Failed to get tenant detail: no response from API")

    return response


def get_tenant_members(tenant_name: str) -> list[TenantMemberResponse]:
    """
    List all members of a tenant with their profiles and access levels.

    Requires the current user to be a member, steward, or admin.

    Args:
        tenant_name: Name of the tenant

    Returns:
        List of TenantMemberResponse with username, display_name, email,
        access_level, and is_steward flag

    Example:
        members = get_tenant_members("kbase")
        for m in members:
            print(f"  {m.username} ({m.display_name}) - {m.access_level}")
    """
    client = get_governance_client()
    response = get_tenant_members_tenants_tenant_name_members_get.sync(client=client, tenant_name=tenant_name)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get tenant members: {_error_message(response)}")

    if not isinstance(response, list):
        raise RuntimeError("Failed to get tenant members: no response from API")

    return response


def get_tenant_stewards(tenant_name: str) -> list[TenantStewardResponse]:
    """
    List stewards of a tenant with their profile details.

    Requires the current user to be a member, steward, or admin.

    Args:
        tenant_name: Name of the tenant

    Returns:
        List of TenantStewardResponse with username, display_name, email,
        assigned_by, and assigned_at

    Example:
        stewards = get_tenant_stewards("kbase")
        for s in stewards:
            print(f"  Steward: {s.username} ({s.display_name})")
    """
    client = get_governance_client()
    response = get_tenant_stewards_tenants_tenant_name_stewards_get.sync(client=client, tenant_name=tenant_name)

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to get tenant stewards: {_error_message(response)}")

    if not isinstance(response, list):
        raise RuntimeError("Failed to get tenant stewards: no response from API")

    return response


def add_tenant_member(
    tenant_name: str,
    username: str,
    permission: str = "read_write",
) -> TenantMemberResponse:
    """
    Add a user to a tenant. Requires steward or admin role.

    Args:
        tenant_name: Name of the tenant
        username: Username to add
        permission: Access level - "read_write" (default) or "read_only"

    Returns:
        TenantMemberResponse for the added user

    Example:
        member = add_tenant_member("kbase", "alice")
        print(f"Added {member.username} with {member.access_level} access")

        # Add as read-only
        member = add_tenant_member("kbase", "bob", permission="read_only")
    """
    if permission not in _PERMISSION_MAP:
        raise ValueError(f"Invalid permission: {permission!r}. Must be 'read_write' or 'read_only'.")

    client = get_governance_client()
    response = add_tenant_member_tenants_tenant_name_members_username_post.sync(
        client=client,
        tenant_name=tenant_name,
        username=username,
        permission=_PERMISSION_MAP[permission],
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to add member {username!r} to tenant {tenant_name!r}: {_error_message(response)}")

    if not isinstance(response, TenantMemberResponse):
        raise RuntimeError(f"Failed to add member {username!r}: no response from API")

    logger.info(f"Added {username} to tenant {tenant_name} with {permission} access")
    return response


def remove_tenant_member(tenant_name: str, username: str) -> None:
    """
    Remove a user from a tenant. Requires steward or admin role.

    Stewards cannot remove other stewards or themselves.

    Args:
        tenant_name: Name of the tenant
        username: Username to remove

    Example:
        remove_tenant_member("kbase", "alice")
    """
    client = get_governance_client()
    response = remove_tenant_member_tenants_tenant_name_members_username_delete.sync(
        client=client,
        tenant_name=tenant_name,
        username=username,
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(
            f"Failed to remove member {username!r} from tenant {tenant_name!r}: {_error_message(response)}"
        )

    logger.info(f"Removed {username} from tenant {tenant_name}")


def update_tenant_metadata(
    tenant_name: str,
    display_name: str | None = None,
    description: str | None = None,
    organization: str | None = None,
) -> TenantMetadataResponse:
    """
    Update tenant metadata. Requires steward or admin role.

    Only provided fields are updated; omitted fields remain unchanged.

    Args:
        tenant_name: Name of the tenant
        display_name: Human-readable name for the tenant
        description: Purpose or description of the tenant
        organization: Organization the tenant belongs to

    Returns:
        Updated TenantMetadataResponse

    Example:
        updated = update_tenant_metadata(
            "kbase",
            description="KBase genomics data team",
            organization="DOE KBase"
        )
        print(f"Updated: {updated.display_name}")
    """
    body = TenantMetadataUpdate(
        display_name=display_name if display_name is not None else UNSET,
        description=description if description is not None else UNSET,
        organization=organization if organization is not None else UNSET,
    )

    client = get_governance_client()
    response = update_tenant_metadata_tenants_tenant_name_patch.sync(
        client=client,
        tenant_name=tenant_name,
        body=body,
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(f"Failed to update tenant metadata: {_error_message(response)}")

    if not isinstance(response, TenantMetadataResponse):
        raise RuntimeError("Failed to update tenant metadata: no response from API")

    logger.info(f"Updated metadata for tenant {tenant_name}")
    return response


def assign_steward(tenant_name: str, username: str) -> TenantStewardResponse:
    """
    Assign a user as steward of a tenant. Admin only.

    The user must already be a member of the tenant.

    Args:
        tenant_name: Name of the tenant
        username: Username to assign as steward

    Returns:
        TenantStewardResponse with username, display_name, email,
        assigned_by, and assigned_at

    Example:
        steward = assign_steward("kbase", "alice")
        print(f"Assigned {steward.username} as steward by {steward.assigned_by}")
    """
    client = get_governance_client()
    response = assign_steward_tenants_tenant_name_stewards_username_post.sync(
        client=client,
        tenant_name=tenant_name,
        username=username,
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(
            f"Failed to assign steward {username!r} for tenant {tenant_name!r}: {_error_message(response)}"
        )

    if not isinstance(response, TenantStewardResponse):
        raise RuntimeError(f"Failed to assign steward {username!r}: no response from API")

    logger.info(f"Assigned {username} as steward of tenant {tenant_name}")
    return response


def remove_steward(tenant_name: str, username: str) -> None:
    """
    Remove a steward assignment from a tenant. Admin only.

    This does not remove the user from the tenant — only the steward role.

    Args:
        tenant_name: Name of the tenant
        username: Username to remove as steward

    Example:
        remove_steward("kbase", "alice")
    """
    client = get_governance_client()
    response = remove_steward_tenants_tenant_name_stewards_username_delete.sync(
        client=client,
        tenant_name=tenant_name,
        username=username,
    )

    if isinstance(response, ErrorResponse):
        raise RuntimeError(
            f"Failed to remove steward {username!r} from tenant {tenant_name!r}: {_error_message(response)}"
        )

    logger.info(f"Removed {username} as steward of tenant {tenant_name}")


def _val(field):
    """Return field value if set, otherwise None."""
    return field if field not in (UNSET, None) else None


def _print_tenant(t: TenantSummaryResponse, detail: TenantDetailResponse, show_databases: bool, all_databases: list[str] | None) -> None:
    """Print formatted detail for a single tenant."""
    meta = detail.metadata
    paths = detail.storage_paths

    # Header
    name = _val(meta.display_name) or t.tenant_name
    roles = []
    if t.is_steward:
        roles.append("steward")
    elif t.is_member:
        roles.append("member")
    role_str = ", ".join(roles) if roles else "---"
    print(f"{'=' * 60}")
    print(f"  {name}  ({t.tenant_name})  [{role_str}]")
    print(f"{'=' * 60}")

    # Metadata
    print(f"  Description : {_val(meta.description) or '-'}")
    print(f"  Organization: {_val(meta.organization) or '-'}")
    print(f"  Created by  : {meta.created_by}  ({meta.created_at:%Y-%m-%d})")

    # Storage
    print(f"\n  Storage:")
    print(f"    General warehouse : {paths.general_warehouse}")
    print(f"    SQL warehouse     : {paths.sql_warehouse}")
    print(f"    Namespace prefix  : {paths.namespace_prefix}")

    # Stewards
    print(f"\n  Stewards ({len(detail.stewards)}):")
    if detail.stewards:
        for s in detail.stewards:
            sname = _val(s.display_name) or s.username
            email = _val(s.email) or "-"
            print(f"    - {sname} ({s.username}) <{email}>")
    else:
        print("    (none)")

    # Members
    print(f"\n  Members ({detail.member_count}):")
    for m in detail.members:
        mname = _val(m.display_name) or m.username
        steward_tag = " [steward]" if m.is_steward else ""
        print(f"    - {mname} ({m.username}) - {m.access_level.value}{steward_tag}")

    # Databases & tables
    if show_databases and all_databases is not None:
        from ..hive_metastore import get_tables as hms_get_tables

        prefix = paths.namespace_prefix
        tenant_dbs = sorted(db for db in all_databases if db.startswith(prefix))
        print(f"\n  Databases ({len(tenant_dbs)}):")
        if tenant_dbs:
            for db in tenant_dbs:
                try:
                    tables = hms_get_tables(db)
                    if tables:
                        print(f"    {db}/ ({len(tables)} tables)")
                        for tbl in sorted(tables):
                            print(f"      - {tbl}")
                    else:
                        print(f"    {db}/ (empty)")
                except Exception as e:
                    print(f"    {db}/ (error: {e})")
        else:
            print(f"    (no databases matching '{prefix}*')")

    print()


def show_my_tenants(show_databases: bool = False) -> None:
    """
    Display tenants for the current user.

    Admins see all tenants, grouped into "My tenants" and "Other tenants".
    Regular users see only tenants they belong to.

    Args:
        show_databases: If True, also list databases and tables matching
                        each tenant's namespace prefix (requires HMS access).

    Example:
        show_my_tenants()                    # without databases
        show_my_tenants(show_databases=True) # with databases and tables
    """
    tenants = list_tenants()
    my_tenants = [t for t in tenants if t.is_member or t.is_steward]
    other_tenants = [t for t in tenants if not t.is_member and not t.is_steward]

    all_databases = None
    if show_databases:
        from ..hive_metastore import get_databases as hms_get_databases

        all_databases = hms_get_databases()

    if my_tenants:
        print(f"My tenants ({len(my_tenants)}):\n")
        for t in my_tenants:
            detail = get_tenant_detail(t.tenant_name)
            _print_tenant(t, detail, show_databases, all_databases)

    if other_tenants:
        print(f"\nOther tenants ({len(other_tenants)}):\n")
        for t in other_tenants:
            detail = get_tenant_detail(t.tenant_name)
            _print_tenant(t, detail, show_databases, all_databases)

    if not tenants:
        print("No tenants found.")
