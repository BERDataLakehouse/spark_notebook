"""Tests for cache.py - Token-dependent cache management."""

from functools import lru_cache

from berdl_notebook_utils.cache import (
    _token_change_caches,
    clear_kbase_token_caches,
    kbase_token_dependent,
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
