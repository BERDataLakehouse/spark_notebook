"""
Token-dependent cache management.

Provides a decorator for registering lru_cache'd functions that should be
invalidated when the KBase auth token changes.
"""

_token_change_caches = []


def clears_on_token_change(func):
    """Register an lru_cache'd function for invalidation when the KBase token changes."""
    _token_change_caches.append(func)
    return func


def clear_token_caches():
    """Clear all registered token-dependent caches."""
    for cached in _token_change_caches:
        cached.cache_clear()
