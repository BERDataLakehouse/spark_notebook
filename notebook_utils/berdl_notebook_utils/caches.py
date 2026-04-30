"""Central registry of in-process TTL caches used across berdl_notebook_utils.

Each domain module owns its :class:`cacheout.Cache` instance (so the TTL,
size, and eviction pressure are tuned per workload), but registers it here
so callers have ONE place to:

* discover every cache and what it stores (:func:`list_caches`)
* clear one cache by name (:func:`clear_cache`)
* clear all caches or a domain prefix (:func:`clear_all_caches`)

This is intentionally separate from
:mod:`berdl_notebook_utils.kbase_token_cache`, which manages
``@lru_cache``'d *client/factory singletons* and their token-rotation
invalidation. The two caching layers serve different purposes:

* ``kbase_token_cache``: object pools (clients, settings) — invalidated
  when the KBase auth token changes.
* ``caches`` (this module): TTL data caches (groups, tenants, databases,
  tables, schemas) — invalidated by TTL or explicit ``force_refresh``.

The token rotation hook in ``kbase_token_cache.clear_berdl_token_caches``
also calls :func:`clear_all_caches` here, so token changes wipe both
layers.

Usage
-----

In a domain module:

    from ..caches import register_cache

    groups_cache = register_cache(
        "minio_governance.groups",
        maxsize=1, ttl_seconds=3600,
        description="Cached get_my_groups() UserGroupsResponse.",
    )

In any caller (notebook, tests, handlers):

    from berdl_notebook_utils.caches import list_caches, clear_all_caches

    list_caches()              # → [{name, ttl_seconds, size, maxsize, description}, ...]
    clear_all_caches()         # nuclear: wipe everything
    clear_all_caches(prefix="spark.data_store.")  # targeted wipe
"""

from dataclasses import dataclass
from threading import RLock
from typing import Any, Optional

from cacheout import Cache


@dataclass
class CacheInfo:
    """Metadata wrapper around a registered :class:`cacheout.Cache`."""

    name: str
    cache: Cache
    ttl_seconds: int
    description: str

    def stats(self) -> dict[str, Any]:
        """Return a JSON-serializable snapshot of this cache's state."""
        return {
            "name": self.name,
            "ttl_seconds": self.ttl_seconds,
            "description": self.description,
            "size": len(self.cache),
            "maxsize": self.cache.maxsize,
        }


_registry: dict[str, CacheInfo] = {}
_lock = RLock()


def register_cache(
    name: str,
    *,
    maxsize: int,
    ttl_seconds: int,
    description: str,
) -> Cache:
    """Create and register a new TTL :class:`cacheout.Cache`, or return the existing one.

    Idempotent by ``name`` so repeated module imports (e.g. in test
    reloads) do not produce duplicate caches.

    Args:
        name: Globally unique cache identifier. Convention:
            ``"<module>.<purpose>"`` — e.g. ``"spark.data_store.tables"``.
            Use a stable prefix per domain so :func:`clear_all_caches`
            with ``prefix=`` works for targeted wipes.
        maxsize: Max number of entries before LRU eviction.
        ttl_seconds: Time-to-live for each entry, in seconds.
        description: Human-readable summary of what's cached. Surfaced by
            :func:`list_caches` for discoverability.

    Returns:
        The registered :class:`cacheout.Cache`. Callers store this in
        their module and use it directly.

    Raises:
        ValueError: If ``name`` is already registered with different
            settings (size or TTL). Re-registering with identical
            settings returns the existing cache.
    """
    with _lock:
        existing = _registry.get(name)
        if existing is not None:
            if existing.ttl_seconds != ttl_seconds or existing.cache.maxsize != maxsize:
                raise ValueError(
                    f"Cache {name!r} already registered with different settings: "
                    f"existing(maxsize={existing.cache.maxsize}, ttl={existing.ttl_seconds}) "
                    f"vs new(maxsize={maxsize}, ttl={ttl_seconds})"
                )
            return existing.cache

        cache: Cache = Cache(maxsize=maxsize, ttl=ttl_seconds)
        _registry[name] = CacheInfo(
            name=name,
            cache=cache,
            ttl_seconds=ttl_seconds,
            description=description,
        )
        return cache


def list_caches() -> list[dict[str, Any]]:
    """Return stats for every registered cache, sorted by name."""
    with _lock:
        return [info.stats() for info in sorted(_registry.values(), key=lambda i: i.name)]


def get_cache(name: str) -> Optional[Cache]:
    """Return the registered cache by name, or ``None`` if unknown."""
    with _lock:
        info = _registry.get(name)
        return info.cache if info is not None else None


def clear_cache(name: str) -> bool:
    """Clear one cache by name. Returns True if found and cleared, False otherwise."""
    with _lock:
        info = _registry.get(name)
        if info is None:
            return False
        info.cache.clear()
        return True


def clear_all_caches(prefix: Optional[str] = None) -> int:
    """Clear all registered caches, optionally filtered by name prefix.

    Args:
        prefix: If given, only clear caches whose name starts with this
            string (e.g. ``"spark.data_store."``).

    Returns:
        Number of caches cleared.
    """
    cleared = 0
    with _lock:
        for name, info in _registry.items():
            if prefix is None or name.startswith(prefix):
                info.cache.clear()
                cleared += 1
    return cleared


def _reset_registry_for_tests() -> None:
    """Drop all registered caches. Test-only — do NOT call from production code."""
    with _lock:
        _registry.clear()
