"""In-process TTL caches for read-heavy governance calls.

The extension's tenants landing page hits list_tenants() and get_my_groups()
on every load, which scales with tenant count. These caches use a 1-hour
TTL to reduce repeated read traffic while keeping staleness bounded.

Mutations in this package call :func:`invalidate_all` after success so
changes made in the same notebook process are reflected on the next read.

The cache instances are owned by this module but registered in the central
:mod:`berdl_notebook_utils.caches` registry so they can be discovered and
wiped uniformly with all other BERDL TTL caches.
"""

from ..caches import clear_all_caches, register_cache

_TTL_SECONDS = 3600
_NAME_PREFIX = "governance."

tenants_cache = register_cache(
    f"{_NAME_PREFIX}tenants",
    maxsize=1,
    ttl_seconds=_TTL_SECONDS,
    description="list_tenants(): cached TenantSummaryResponse list for the current user.",
)
groups_cache = register_cache(
    f"{_NAME_PREFIX}groups",
    maxsize=1,
    ttl_seconds=_TTL_SECONDS,
    description="get_my_groups(): cached UserGroupsResponse for the current user.",
)


def invalidate_all() -> None:
    """Clear all governance caches (tenants + groups)."""
    clear_all_caches(prefix=_NAME_PREFIX)
