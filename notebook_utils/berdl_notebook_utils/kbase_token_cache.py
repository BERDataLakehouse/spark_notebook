"""KBase auth token cache management.

Manages two related concerns tied to KBase auth tokens:

1. Registry of ``@lru_cache``'d functions whose results depend on the
   current KBase token (e.g. authenticated client factories) — see
   :func:`kbase_token_dependent`. These are wiped via
   :func:`clear_kbase_token_caches` when the token rotates.

2. The token-sync watcher that picks up token changes from
   ``~/.berdl_kbase_session`` and pushes them into ``os.environ``, then
   triggers cache invalidation — see
   :func:`sync_kbase_token_from_cache_file`.

Token rotation also wipes all TTL caches registered in
:mod:`berdl_notebook_utils.caches` (groups, tenants, table schemas, etc.)
because their contents may have been computed with the prior token.

This module was previously named ``cache.py``; it was renamed to
``kbase_token_cache`` to clearly distinguish it from the general TTL
cache registry in :mod:`berdl_notebook_utils.caches`.
"""

import os
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import TypeVar

from .caches import clear_all_caches as _clear_all_ttl_caches

TOKEN_CACHE_FILE = ".berdl_kbase_session"

F = TypeVar("F", bound=Callable)

_token_change_caches = []
_token_cache_file_snapshot: tuple[Path, int, int, int, int, int] | None = None
_token_cache_env_token: str | None = None


def kbase_token_dependent(func):
    """Register an lru_cache'd function for invalidation when the KBase token changes."""
    _token_change_caches.append(func)
    return func


def clear_kbase_token_caches():
    """Clear all registered token-dependent caches."""
    for cached in _token_change_caches:
        cached.cache_clear()


def _get_token_cache_path() -> Path:
    return Path.home() / TOKEN_CACHE_FILE


def clear_berdl_token_caches() -> None:
    """Clear all in-process BERDL caches in response to a token change.

    Wipes both:

    * ``@lru_cache``'d client/factory functions registered via
      :func:`kbase_token_dependent`, and
    * every TTL cache registered in :mod:`berdl_notebook_utils.caches`
      (governance, data_store, etc.) whose content may have been computed
      with the prior token.
    """
    clear_kbase_token_caches()
    _clear_all_ttl_caches()


def sync_kbase_token_from_cache_file(path: Path | None = None) -> bool:
    """Sync KBASE_AUTH_TOKEN from ~/.berdl_kbase_session when it changed.

    Returns True when the process environment was updated.
    """
    global _token_cache_env_token, _token_cache_file_snapshot

    try:
        token_path = path if path is not None else _get_token_cache_path()
        token_stat = token_path.stat()
    except (OSError, RuntimeError):
        _token_cache_file_snapshot = None
        return False

    file_snapshot = (
        token_path,
        token_stat.st_dev,
        token_stat.st_ino,
        token_stat.st_ctime_ns,
        token_stat.st_mtime_ns,
        token_stat.st_size,
    )
    env_token = os.environ.get("KBASE_AUTH_TOKEN", "")

    if file_snapshot == _token_cache_file_snapshot and env_token == _token_cache_env_token:
        return False

    try:
        token = token_path.read_text().strip()
    except (OSError, UnicodeDecodeError):
        _token_cache_file_snapshot = file_snapshot
        _token_cache_env_token = env_token
        return False

    _token_cache_file_snapshot = file_snapshot
    if not token or token == env_token:
        _token_cache_env_token = env_token
        return False

    os.environ["KBASE_AUTH_TOKEN"] = token
    _token_cache_env_token = token
    clear_berdl_token_caches()
    return True


def sync_kbase_token_before_call(func: F) -> F:
    """Refresh the process token before a cached function is accessed.

    When combining with ``lru_cache``, this must be the outermost decorator:

        @sync_kbase_token_before_call
        @kbase_token_dependent
        @lru_cache(maxsize=1)
        def get_client(...):
            ...

    If ``lru_cache`` wraps this decorator instead, cache hits bypass the wrapper
    and the token sync will not run.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        sync_kbase_token_from_cache_file()
        return func(*args, **kwargs)

    for attr in ("cache_clear", "cache_info", "cache_parameters"):
        if hasattr(func, attr):
            setattr(wrapper, attr, getattr(func, attr))

    return wrapper
