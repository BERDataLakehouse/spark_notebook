"""
Tests for hive_metastore.py module.

Exercises both the public ``get_databases`` / ``get_tables`` helpers and the
underlying :class:`HMSClientPool` invariants (no shared client across
concurrent callers, broken connections are disposed not reused, pool
exhaustion surfaces as a fast failure).
"""

import threading
import time
from contextlib import contextmanager
from unittest.mock import Mock, patch

import pytest

import berdl_notebook_utils.clients as clients_module
from berdl_notebook_utils.hive_metastore import get_databases, get_tables
from berdl_notebook_utils.hms_pool import HMSClientPool, HMSPoolClosed, HMSPoolExhausted


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _reset_pool_cache():
    """Clear the lru_cache on get_hive_metastore_pool so each test gets a fresh pool."""
    clients_module.get_hive_metastore_pool.cache_clear()


@pytest.fixture(autouse=True)
def _isolate_pool():
    _reset_pool_cache()
    yield
    _reset_pool_cache()


@contextmanager
def _patch_pool_with(client_factory):
    """Patch ``HMSClientPool._new_client`` so the pool yields
    ``client_factory()`` instead of opening a real TCP socket, while still
    maintaining the pool's ``created`` metric (so tests can assert on it)."""

    def fake_new_client(self):
        client = client_factory()
        with self._metrics_lock:
            self._created += 1
        return client

    with patch.object(HMSClientPool, "_new_client", autospec=True, side_effect=fake_new_client):
        yield


# ---------------------------------------------------------------------------
# get_databases / get_tables happy-path and error-path
# ---------------------------------------------------------------------------


class TestGetDatabases:
    def test_returns_list(self):
        client = Mock()
        client.get_databases.return_value = ["db1", "db2", "db3"]
        with _patch_pool_with(lambda: client):
            assert get_databases() == ["db1", "db2", "db3"]
        client.get_databases.assert_called_once_with("*")

    def test_empty_list(self):
        client = Mock()
        client.get_databases.return_value = []
        with _patch_pool_with(lambda: client):
            assert get_databases() == []

    def test_disposes_on_error(self):
        """An exception inside ``acquire`` must dispose the connection,
        never return it to the idle pool."""
        client = Mock()
        client.get_databases.side_effect = Exception("Connection error")
        with _patch_pool_with(lambda: client):
            with pytest.raises(Exception, match="Connection error"):
                get_databases()
            # Connection was disposed and not parked back into idle.
            pool = clients_module.get_hive_metastore_pool()
            assert pool.idle_count == 0
            assert pool.disposed == 1
        client.close.assert_called_once()


class TestGetTables:
    def test_returns_list(self):
        client = Mock()
        client.get_tables.return_value = ["table1", "table2"]
        with _patch_pool_with(lambda: client):
            assert get_tables("test_db") == ["table1", "table2"]
        client.get_tables.assert_called_once_with("test_db", "*")

    def test_empty_database(self):
        client = Mock()
        client.get_tables.return_value = []
        with _patch_pool_with(lambda: client):
            assert get_tables("empty_db") == []

    def test_disposes_on_error(self):
        client = Mock()
        client.get_tables.side_effect = Exception("Database not found")
        with _patch_pool_with(lambda: client):
            with pytest.raises(Exception, match="Database not found"):
                get_tables("nonexistent_db")
            pool = clients_module.get_hive_metastore_pool()
            assert pool.idle_count == 0
            assert pool.disposed == 1
        client.close.assert_called_once()


# ---------------------------------------------------------------------------
# HMSClientPool — direct unit tests of pool invariants
# ---------------------------------------------------------------------------


def _make_fake_client():
    """Cheap fake that records open/close and supports the methods we use."""
    c = Mock()
    c.opened = True
    return c


class TestHMSClientPool:
    def test_reuses_idle_connection(self):
        clients_created = []

        def factory():
            c = _make_fake_client()
            clients_created.append(c)
            return c

        with _patch_pool_with(factory):
            pool = clients_module.get_hive_metastore_pool()
            with pool.acquire() as c1:
                c1.get_databases("*")
            with pool.acquire() as c2:
                c2.get_databases("*")
            # Same connection reused → exactly one constructed.
            assert pool.created == 1
            assert pool.disposed == 0
            assert c1 is c2

    def test_max_size_limits_concurrency(self):
        """No more than ``max_size`` connections are constructed even under
        sustained concurrent acquire calls."""

        def factory():
            return _make_fake_client()

        # Build a small pool directly so we can pick a tiny size.
        with _patch_pool_with(factory):
            pool = HMSClientPool(host="x", port=1, max_size=4, acquire_timeout_s=2.0)
            barrier = threading.Barrier(4)

            def worker():
                with pool.acquire():
                    barrier.wait(timeout=2)
                    time.sleep(0.05)

            threads = [threading.Thread(target=worker) for _ in range(16)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=10)
            assert pool.created <= 4
            assert pool.in_use == 0

    def test_acquire_timeout_raises_pool_exhausted(self):
        with _patch_pool_with(lambda: _make_fake_client()):
            pool = HMSClientPool(host="x", port=1, max_size=1, acquire_timeout_s=0.1)
            with pool.acquire():
                with pytest.raises(HMSPoolExhausted):
                    with pool.acquire():
                        pass

    def test_close_blocks_further_acquires(self):
        with _patch_pool_with(lambda: _make_fake_client()):
            pool = HMSClientPool(host="x", port=1, max_size=2, acquire_timeout_s=0.1)
            pool.close()
            with pytest.raises(HMSPoolClosed):
                with pool.acquire():
                    pass

    def test_close_disposes_idle_connections(self):
        with _patch_pool_with(lambda: _make_fake_client()):
            pool = HMSClientPool(host="x", port=1, max_size=2, acquire_timeout_s=0.1)
            with pool.acquire():
                pass
            assert pool.idle_count == 1
            pool.close()
            assert pool.idle_count == 0
            assert pool.disposed == 1

    def test_idle_max_recycles_old_connections(self):
        with _patch_pool_with(lambda: _make_fake_client()):
            pool = HMSClientPool(host="x", port=1, max_size=2, acquire_timeout_s=0.1, idle_max_s=0.01)
            with pool.acquire():
                pass
            time.sleep(0.05)  # exceed idle_max_s
            with pool.acquire():
                pass
            # Old connection was recycled; a new one was created in its place.
            assert pool.created == 2
            assert pool.disposed == 1


class TestHMSClientPoolConcurrency:
    """Regression test for the 2026-04-29 ``UnicodeDecodeError`` incident.

    Each concurrent caller MUST receive its own client instance; no two
    threads may ever hold the same client at once. With the old singleton
    (``@lru_cache(maxsize=1)`` on ``get_hive_metastore_client``) this
    invariant was violated, causing interleaved Thrift frames on a shared
    socket and ``UnicodeDecodeError: ... byte 0x80 in position 5``.
    """

    def test_no_two_threads_share_a_client(self):
        in_use_lock = threading.Lock()
        in_use: set[int] = set()
        violations: list[str] = []

        def factory():
            return _make_fake_client()

        with _patch_pool_with(factory):
            pool = HMSClientPool(host="x", port=1, max_size=8, acquire_timeout_s=5.0)

            def worker(_i: int):
                for _ in range(50):
                    with pool.acquire() as client:
                        cid = id(client)
                        with in_use_lock:
                            if cid in in_use:
                                violations.append(f"client {cid} already checked out")
                            in_use.add(cid)
                        # Simulate work that races with other threads.
                        time.sleep(0.001)
                        with in_use_lock:
                            in_use.discard(cid)

            threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            assert violations == [], f"client sharing detected ({len(violations)} violations): {violations[:5]}"
            assert pool.in_use == 0
            assert pool.created <= pool.max_size

    def test_disposes_exception_path_under_concurrency(self):
        """Even under concurrent failures, exceptions never leave a half-broken
        connection in the idle pool."""

        def factory():
            c = _make_fake_client()
            c.get_databases.side_effect = RuntimeError("simulated thrift corruption")
            return c

        with _patch_pool_with(factory):
            pool = HMSClientPool(host="x", port=1, max_size=4, acquire_timeout_s=2.0)

            def worker():
                for _ in range(10):
                    try:
                        with pool.acquire() as client:
                            client.get_databases("*")
                    except RuntimeError:
                        pass

            threads = [threading.Thread(target=worker) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=15)

            # Every checked-out connection that saw an exception was disposed.
            assert pool.idle_count == 0
            assert pool.in_use == 0
            assert pool.created == pool.disposed
