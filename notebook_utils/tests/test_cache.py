"""Tests for cache.py - Token-dependent cache management."""

from functools import lru_cache
import os
import sys
from types import SimpleNamespace

from berdl_notebook_utils.cache import (
    _token_change_caches,
    clear_berdl_token_caches,
    clear_kbase_token_caches,
    kbase_token_dependent,
    sync_kbase_token_before_call,
    sync_kbase_token_from_cache_file,
)


class TestKbaseTokenDependent:
    """Tests for kbase_token_dependent decorator."""

    def test_registers_function(self):
        """Test that decorator registers the function in _token_change_caches."""
        initial_len = len(_token_change_caches)

        try:
            # Production order: @kbase_token_dependent on top of @lru_cache
            # This means lru_cache wraps first, then kbase_token_dependent registers the wrapper
            @kbase_token_dependent
            @lru_cache
            def dummy_func():
                return "value"

            assert len(_token_change_caches) == initial_len + 1
            assert dummy_func in _token_change_caches
        finally:
            # Clean up without assuming registration always occurred
            if "dummy_func" in locals() and dummy_func in _token_change_caches:
                _token_change_caches.remove(dummy_func)

    def test_returns_function_unchanged(self):
        """Test that decorator returns the function without modification."""

        @lru_cache
        def original():
            return 42

        try:
            result = kbase_token_dependent(original)
            assert result is original
            assert result() == 42
        finally:
            # Clean up without assuming registration always occurred
            if original in _token_change_caches:
                _token_change_caches.remove(original)


class TestClearKbaseTokenCaches:
    """Tests for clear_kbase_token_caches function."""

    def test_clears_all_registered_caches(self):
        """Test that clear_kbase_token_caches calls cache_clear on all registered functions."""
        call_count = 0

        @lru_cache
        def cached_func():
            nonlocal call_count
            call_count += 1
            return call_count

        _token_change_caches.append(cached_func)

        try:
            # Call once to populate cache
            result1 = cached_func()
            assert result1 == 1

            # Call again - should return cached value
            result2 = cached_func()
            assert result2 == 1

            # Clear caches
            clear_kbase_token_caches()

            # Call again - should recompute
            result3 = cached_func()
            assert result3 == 2
        finally:
            _token_change_caches.remove(cached_func)

    def test_handles_multiple_caches(self):
        """Test clearing multiple registered caches."""

        @lru_cache
        def func_a():
            return "a"

        @lru_cache
        def func_b():
            return "b"

        _token_change_caches.append(func_a)
        _token_change_caches.append(func_b)

        try:
            # Populate caches
            func_a()
            func_b()
            assert func_a.cache_info().hits == 0
            assert func_b.cache_info().hits == 0

            func_a()
            func_b()
            assert func_a.cache_info().hits == 1
            assert func_b.cache_info().hits == 1

            # Clear all
            clear_kbase_token_caches()

            # Verify caches were cleared
            assert func_a.cache_info().hits == 0
            assert func_a.cache_info().misses == 0
            assert func_b.cache_info().hits == 0
            assert func_b.cache_info().misses == 0
        finally:
            _token_change_caches.remove(func_a)
            _token_change_caches.remove(func_b)


class TestClearBerdlTokenCaches:
    """Tests for clearing all BERDL token-dependent caches."""

    def test_clears_token_and_loaded_governance_caches(self, monkeypatch):
        calls: list[str] = []

        @lru_cache
        def cached_func():
            return "value"

        _token_change_caches.append(cached_func)
        monkeypatch.setitem(
            sys.modules,
            "berdl_notebook_utils.minio_governance._cache",
            SimpleNamespace(invalidate_all=lambda: calls.append("governance")),
        )

        try:
            cached_func()
            cached_func()
            assert cached_func.cache_info().hits == 1

            clear_berdl_token_caches()

            assert cached_func.cache_info().hits == 0
            assert calls == ["governance"]
        finally:
            _token_change_caches.remove(cached_func)


class TestSyncKbaseTokenFromCacheFile:
    """Tests for syncing KBase token from the server-written cache file."""

    def test_updates_env_and_clears_token_caches(self, tmp_path, monkeypatch):
        call_count = 0

        @lru_cache
        def cached_func():
            nonlocal call_count
            call_count += 1
            return call_count

        _token_change_caches.append(cached_func)
        token_file = tmp_path / ".berdl_kbase_session"
        token_file.write_text("new-token")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "old-token")

        try:
            assert cached_func() == 1
            assert cached_func() == 1

            assert sync_kbase_token_from_cache_file(token_file) is True

            assert os.environ["KBASE_AUTH_TOKEN"] == "new-token"
            assert cached_func() == 2
        finally:
            _token_change_caches.remove(cached_func)

    def test_clears_loaded_governance_caches(self, tmp_path, monkeypatch):
        calls: list[str] = []
        token_file = tmp_path / ".berdl_kbase_session"
        token_file.write_text("new-token")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "old-token")
        monkeypatch.setitem(
            sys.modules,
            "berdl_notebook_utils.minio_governance._cache",
            SimpleNamespace(invalidate_all=lambda: calls.append("governance")),
        )

        assert sync_kbase_token_from_cache_file(token_file) is True

        assert calls == ["governance"]

    def test_noops_when_token_is_unchanged(self, tmp_path, monkeypatch):
        token_file = tmp_path / ".berdl_kbase_session"
        token_file.write_text("same-token")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "same-token")

        assert sync_kbase_token_from_cache_file(token_file) is False

    def test_noops_when_token_file_is_missing(self, tmp_path):
        assert sync_kbase_token_from_cache_file(tmp_path / "missing") is False


class TestSyncKbaseTokenBeforeCall:
    """Tests for the pre-call token sync decorator."""

    def test_runs_sync_before_lru_cache_lookup(self, tmp_path, monkeypatch):
        calls: list[str] = []
        token_file = tmp_path / ".berdl_kbase_session"
        token_file.write_text("fresh-token")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "old-token")
        monkeypatch.setattr(
            "berdl_notebook_utils.cache._get_token_cache_path",
            lambda: token_file,
        )

        @sync_kbase_token_before_call
        @lru_cache
        def cached_func():
            calls.append(os.environ["KBASE_AUTH_TOKEN"])
            return os.environ["KBASE_AUTH_TOKEN"]

        assert cached_func() == "fresh-token"
        assert cached_func() == "fresh-token"
        assert calls == ["fresh-token"]
