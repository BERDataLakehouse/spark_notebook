"""
Token-dependent cache management.

Provides a decorator for registering lru_cache'd functions that should be
invalidated when the KBase auth token changes.
"""

import os
import sys
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import TypeVar

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


def _clear_loaded_governance_caches() -> None:
    module = sys.modules.get("berdl_notebook_utils.minio_governance._cache")
    if module is None:
        return

    invalidate_all = getattr(module, "invalidate_all", None)
    if callable(invalidate_all):
        invalidate_all()


def clear_berdl_token_caches() -> None:
    """Clear all token-dependent BERDL caches in this process."""
    clear_kbase_token_caches()
    _clear_loaded_governance_caches()


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
    """Decorator that refreshes the process token before cache lookup/use."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        sync_kbase_token_from_cache_file()
        return func(*args, **kwargs)

    for attr in ("cache_clear", "cache_info", "cache_parameters"):
        if hasattr(func, attr):
            setattr(wrapper, attr, getattr(func, attr))

    return wrapper
