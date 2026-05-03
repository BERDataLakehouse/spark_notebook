"""Tests for berdl_notebook_utils.caches — the central TTL cache registry."""

import pytest

from berdl_notebook_utils import caches


@pytest.fixture(autouse=True)
def _isolate_registry():
    """Snapshot/restore the central registry around each test for isolation."""
    original = dict(caches._registry)
    yield
    caches._registry.clear()
    caches._registry.update(original)


class TestRegisterCache:
    """Tests for register_cache()."""

    def test_creates_and_registers_new_cache(self):
        cache = caches.register_cache("test.alpha", maxsize=8, ttl_seconds=30, description="alpha test")
        cache.set("k", "v")

        assert cache.get("k") == "v"
        assert "test.alpha" in {info["name"] for info in caches.list_caches()}

    def test_idempotent_when_settings_match(self):
        first = caches.register_cache("test.alpha", maxsize=8, ttl_seconds=30, description="alpha")
        second = caches.register_cache("test.alpha", maxsize=8, ttl_seconds=30, description="alpha (re-import)")

        assert first is second  # same Cache instance returned

    def test_raises_when_settings_differ(self):
        caches.register_cache("test.alpha", maxsize=8, ttl_seconds=30, description="alpha")
        with pytest.raises(ValueError, match="different settings"):
            caches.register_cache("test.alpha", maxsize=16, ttl_seconds=30, description="conflict")

        with pytest.raises(ValueError, match="different settings"):
            caches.register_cache("test.alpha", maxsize=8, ttl_seconds=60, description="conflict")


class TestListCaches:
    """Tests for list_caches() reporting."""

    def test_returns_stats_for_all_registered(self):
        c1 = caches.register_cache("test.beta", maxsize=4, ttl_seconds=10, description="beta")
        c1.set("a", 1)
        c1.set("b", 2)
        caches.register_cache("test.alpha", maxsize=2, ttl_seconds=5, description="alpha")

        stats = caches.list_caches()
        # Sorted by name → alpha first, then beta
        names = [s["name"] for s in stats if s["name"].startswith("test.")]
        assert names == ["test.alpha", "test.beta"]

        beta = next(s for s in stats if s["name"] == "test.beta")
        assert beta["size"] == 2
        assert beta["maxsize"] == 4
        assert beta["ttl_seconds"] == 10
        assert beta["description"] == "beta"


class TestClearCache:
    """Tests for clear_cache() (single-cache invalidation)."""

    def test_clears_named_cache(self):
        cache = caches.register_cache("test.gamma", maxsize=4, ttl_seconds=30, description="g")
        cache.set("k", "v")

        assert caches.clear_cache("test.gamma") is True
        assert cache.get("k") is None

    def test_returns_false_when_unknown(self):
        assert caches.clear_cache("test.does_not_exist") is False


class TestClearAllCaches:
    """Tests for clear_all_caches() global / prefix-scoped invalidation."""

    def test_clears_everything_when_no_prefix(self):
        c1 = caches.register_cache("test.one", maxsize=4, ttl_seconds=30, description="1")
        c2 = caches.register_cache("test.two", maxsize=4, ttl_seconds=30, description="2")
        c1.set("a", 1)
        c2.set("b", 2)

        cleared = caches.clear_all_caches()

        assert cleared >= 2  # may include caches registered by other modules at import
        assert c1.get("a") is None
        assert c2.get("b") is None

    def test_prefix_only_clears_matching(self):
        keep = caches.register_cache("other.module", maxsize=4, ttl_seconds=30, description="keep")
        wipe = caches.register_cache("test.scoped.x", maxsize=4, ttl_seconds=30, description="wipe x")
        wipe2 = caches.register_cache("test.scoped.y", maxsize=4, ttl_seconds=30, description="wipe y")
        keep.set("k", "v")
        wipe.set("k", "v")
        wipe2.set("k", "v")

        cleared = caches.clear_all_caches(prefix="test.scoped.")

        assert cleared == 2
        assert keep.get("k") == "v"
        assert wipe.get("k") is None
        assert wipe2.get("k") is None


class TestGetCache:
    """Tests for get_cache() lookup."""

    def test_returns_registered_cache(self):
        cache = caches.register_cache("test.delta", maxsize=4, ttl_seconds=30, description="d")
        assert caches.get_cache("test.delta") is cache

    def test_returns_none_when_unknown(self):
        assert caches.get_cache("test.nope") is None


class TestRegistryIntegration:
    """Verify production modules pre-register their caches at import time."""

    def test_governance_caches_are_registered(self):
        # Import triggers registration as a side effect.
        from berdl_notebook_utils.governance import _cache  # noqa: F401

        names = {info["name"] for info in caches.list_caches()}
        assert "governance.tenants" in names
        assert "governance.groups" in names

    def test_spark_data_store_caches_are_registered(self):
        from berdl_notebook_utils.spark import _cache  # noqa: F401

        names = {info["name"] for info in caches.list_caches()}
        assert "spark.data_store.databases" in names
        assert "spark.data_store.tables" in names
        assert "spark.data_store.schema" in names
        assert "spark.data_store.structure" in names
