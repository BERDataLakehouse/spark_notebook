"""Module for querying Hive metastore information using the HMS client pool.

Concurrency note
----------------
``hmsclient.HMSClient`` is a Thrift client and is **not thread-safe**: a
single instance serializes one in-flight call and concurrent use corrupts
the underlying TCP socket. These helpers acquire a fresh connection from
the per-process :class:`berdl_notebook_utils.hms_pool.HMSClientPool` for
each call, so they are safe to call from multiple threads — for example
from the ``ThreadPoolExecutor`` that backs JupyterServer's
``loop.run_in_executor``.
"""

from berdl_notebook_utils.clients import get_hive_metastore_pool


def get_databases() -> list[str]:
    """Get list of databases from Hive metastore.

    Returns:
        List of database names.
    """
    with get_hive_metastore_pool().acquire() as client:
        return client.get_databases("*")


def get_tables(database: str) -> list[str]:
    """Get list of tables in a database from Hive metastore.

    Args:
        database: Name of the database.

    Returns:
        List of table names.
    """
    with get_hive_metastore_pool().acquire() as client:
        return client.get_tables(database, "*")
