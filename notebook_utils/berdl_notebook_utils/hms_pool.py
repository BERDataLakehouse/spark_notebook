"""
Bounded, thread-safe connection pool for Apache Hive Metastore (Thrift) clients.

Background
----------
``hmsclient.HMSClient`` extends Apache Thrift's ``TClient``. A single client
instance owns one TCP socket, one ``TBufferedTransport``, one ``TBinaryProtocol``,
and a shared ``_seqid`` counter that is incremented per call without locking.
This means **a single client instance is not thread-safe**: concurrent callers
will write Thrift frames into the same socket and read each other's responses,
which surfaces as ``UnicodeDecodeError`` while parsing reply strings or
``TApplicationException: <method> failed: unknown result``. Once that happens
the underlying socket is left in an inconsistent state and every subsequent call
hangs.

The pool below preserves the property "one in-flight call per client" while
allowing many concurrent callers in the same process. Key invariants:

* ``BoundedSemaphore`` enforces ``max_size`` concurrent in-flight calls.
* Idle connections are reused (LIFO) to keep working set small.
* ``TSocket.setTimeout`` sets BOTH connect and read timeouts at the socket
  layer, so a wedged peer cannot block an executor thread indefinitely.
* Any exception raised while a connection is checked out **disposes** the
  connection rather than returning it to the pool — a half-broken socket can
  never be reused.
* Idle connections older than ``idle_max_s`` are recycled to bound socket age.
* Pool exhaustion surfaces as ``HMSPoolExhausted`` after ``acquire_timeout_s``,
  rather than blocking forever.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from contextlib import contextmanager
from typing import Iterator
from urllib.parse import urlparse

from hmsclient import HMSClient
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from berdl_notebook_utils import BERDLSettings, get_settings

logger = logging.getLogger(__name__)


DEFAULT_MAX_SIZE = 8
DEFAULT_ACQUIRE_TIMEOUT_S = 5.0
DEFAULT_SOCKET_TIMEOUT_MS = 10_000
DEFAULT_IDLE_MAX_S = 60.0


class HMSPoolExhausted(RuntimeError):
    """Raised when no HMS connection becomes available within ``acquire_timeout_s``."""


class HMSPoolClosed(RuntimeError):
    """Raised when ``acquire`` is called on a pool that has been closed."""


class HMSClientPool:
    """Bounded, thread-safe pool of ``HMSClient`` instances.

    See module docstring for the rationale and invariants.

    Args:
        host: Hive Metastore Thrift host.
        port: Hive Metastore Thrift port.
        max_size: Maximum number of concurrent in-flight HMS connections.
        acquire_timeout_s: Maximum time to wait for a free slot before raising
            :class:`HMSPoolExhausted`.
        socket_timeout_ms: Per-socket connect AND read timeout in milliseconds.
            Applied via ``TSocket.setTimeout`` so that wedged peers do not
            block a worker thread indefinitely.
        idle_max_s: Maximum age (in seconds) of an idle connection before it
            is recycled instead of reused.
    """

    def __init__(
        self,
        host: str,
        port: int,
        max_size: int = DEFAULT_MAX_SIZE,
        acquire_timeout_s: float = DEFAULT_ACQUIRE_TIMEOUT_S,
        socket_timeout_ms: int = DEFAULT_SOCKET_TIMEOUT_MS,
        idle_max_s: float = DEFAULT_IDLE_MAX_S,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        if acquire_timeout_s <= 0:
            raise ValueError("acquire_timeout_s must be > 0")
        if socket_timeout_ms <= 0:
            raise ValueError("socket_timeout_ms must be > 0")
        if idle_max_s <= 0:
            raise ValueError("idle_max_s must be > 0")

        self._host = host
        self._port = int(port)
        self._max_size = max_size
        self._acquire_timeout_s = acquire_timeout_s
        self._socket_timeout_ms = socket_timeout_ms
        self._idle_max_s = idle_max_s

        self._sem = threading.BoundedSemaphore(max_size)
        # LIFO so we reuse hot connections preferentially and let cold ones age out.
        self._idle: queue.LifoQueue[tuple[HMSClient, float]] = queue.LifoQueue(maxsize=max_size)
        self._closed = False

        # Counters protected by ``_metrics_lock``. Exposed for tests and for
        # future Prometheus metric exporters.
        self._metrics_lock = threading.Lock()
        self._created = 0
        self._disposed = 0
        self._in_use = 0

    # ------------------------------------------------------------------
    # Metrics (read-only views)
    # ------------------------------------------------------------------

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def created(self) -> int:
        with self._metrics_lock:
            return self._created

    @property
    def disposed(self) -> int:
        with self._metrics_lock:
            return self._disposed

    @property
    def in_use(self) -> int:
        with self._metrics_lock:
            return self._in_use

    @property
    def idle_count(self) -> int:
        return self._idle.qsize()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_client(self) -> HMSClient:
        """Construct a configured but **unopened** ``HMSClient``.

        The transport carries the pool's socket-level connect+read timeout
        (``socket_timeout_ms``) so that even callers who use this client
        outside the pool — e.g. the deprecated
        :func:`berdl_notebook_utils.clients.get_hive_metastore_client` — get
        the same wedge-protection. The caller is responsible for ``open()``
        and ``close()``.
        """
        sock = TSocket.TSocket(self._host, self._port)
        # Applies to both connect() and recv() at the socket layer. Without
        # this, a wedged peer can block a worker thread forever.
        sock.setTimeout(self._socket_timeout_ms)
        transport = TTransport.TBufferedTransport(sock)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return HMSClient(iprot=protocol)

    def _new_client(self) -> HMSClient:
        """Construct, open, and account for a new pool-owned ``HMSClient``."""
        client = self._build_client()
        client.open()
        with self._metrics_lock:
            self._created += 1
        return client

    def _dispose(self, client: HMSClient) -> None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - close() on a broken socket can raise; we don't care.
            logger.debug("Error closing HMS client (likely already broken)", exc_info=True)
        finally:
            with self._metrics_lock:
                self._disposed += 1

    def _try_get_idle(self) -> HMSClient | None:
        """Pop an idle connection from the cache, recycling stale ones."""
        while True:
            try:
                client, last_used = self._idle.get_nowait()
            except queue.Empty:
                return None
            if (time.monotonic() - last_used) > self._idle_max_s:
                # Aged out — dispose and try the next one.
                self._dispose(client)
                continue
            return client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @contextmanager
    def acquire(self) -> Iterator[HMSClient]:
        """Check out a connected ``HMSClient`` for the duration of the block.

        Raises:
            HMSPoolClosed: if the pool has been closed.
            HMSPoolExhausted: if no connection becomes available within
                ``acquire_timeout_s``.
        """
        if self._closed:
            raise HMSPoolClosed("HMSClientPool is closed")

        if not self._sem.acquire(timeout=self._acquire_timeout_s):
            raise HMSPoolExhausted(
                f"could not acquire HMS connection within {self._acquire_timeout_s}s (max_size={self._max_size})"
            )

        client: HMSClient | None = None
        returned = False
        try:
            client = self._try_get_idle() or self._new_client()
            with self._metrics_lock:
                self._in_use += 1

            yield client

            # Successful path: return the connection to the idle cache, unless
            # the pool was closed while it was checked out (in which case we
            # must dispose to honour close()'s contract — no idle sockets must
            # remain after the pool is closed).
            if self._closed:
                self._dispose(client)
                returned = True
            else:
                try:
                    self._idle.put_nowait((client, time.monotonic()))
                    returned = True
                except queue.Full:
                    # Pool was resized down or another caller raced us; dispose.
                    self._dispose(client)
        except Exception:
            # Any error while the connection was checked out: dispose. We
            # cannot safely reuse a connection whose protocol state is unknown.
            if client is not None and not returned:
                self._dispose(client)
                returned = True
            raise
        finally:
            if client is not None:
                with self._metrics_lock:
                    self._in_use -= 1
            self._sem.release()

    def close(self) -> None:
        """Close the pool and dispose all idle connections.

        In-flight checkouts are not interrupted; their connections will be
        disposed when they are released.
        """
        self._closed = True
        while True:
            try:
                client, _ = self._idle.get_nowait()
            except queue.Empty:
                return
            self._dispose(client)


def build_pool_from_settings(settings: BERDLSettings | None = None) -> HMSClientPool:
    """Construct an :class:`HMSClientPool` from BERDL settings.

    Reads ``BERDL_HIVE_METASTORE_URI`` (``thrift://host:port``) and the
    optional pool tuning environment variables documented on
    :class:`berdl_notebook_utils.berdl_settings.BERDLSettings`.
    """
    settings = settings or get_settings()
    if settings.BERDL_HIVE_METASTORE_URI is None:
        raise RuntimeError("BERDL_HIVE_METASTORE_URI is not set; cannot build HMSClientPool")

    parsed = urlparse(str(settings.BERDL_HIVE_METASTORE_URI))
    host = parsed.hostname
    port = parsed.port
    if not host or not port:
        raise RuntimeError(
            f"BERDL_HIVE_METASTORE_URI={settings.BERDL_HIVE_METASTORE_URI!r} must be of the form thrift://host:port"
        )

    return HMSClientPool(host=host, port=port)
