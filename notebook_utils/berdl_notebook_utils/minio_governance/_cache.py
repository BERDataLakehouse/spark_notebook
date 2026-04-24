"""In-process TTL caches for read-heavy governance calls.

The extension's tenants landing page hits list_tenants() and get_my_groups()
on every load, which scales with tenant count. These caches match the 300s
TTL used elsewhere in the monorepo (trino_access_control plugin,
minio_manager_service/kbase_profile_client) so staleness windows are
consistent across the platform.

Mutations in this package call invalidate_all() after success so changes
made in the same notebook process are reflected on the next read.
"""

from cacheout import Cache

_TTL_SECONDS = 3600

tenants_cache: Cache = Cache(maxsize=1, ttl=_TTL_SECONDS)
groups_cache: Cache = Cache(maxsize=1, ttl=_TTL_SECONDS)


def invalidate_all() -> None:
    tenants_cache.clear()
    groups_cache.clear()
