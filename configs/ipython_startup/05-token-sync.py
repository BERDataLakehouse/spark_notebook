"""
Background Token Sync for Kernel Processes
==========================================

Starts a daemon thread that reads ~/.berdl_kbase_session every 30 seconds
and updates os.environ["KBASE_AUTH_TOKEN"] in this kernel process when it
changes.

When the token changes, all lru_cache'd client factories are invalidated so
they rebuild with the fresh token on next use.

If you add a new @lru_cache client factory that uses KBASE_AUTH_TOKEN,
add its cache_clear() call to _clear_client_caches() below.
"""

import logging
import os
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)

TOKEN_CACHE_FILE = ".berdl_kbase_session"
TOKEN_SYNC_INTERVAL_SECONDS = 30


def _clear_client_caches():
    """Clear all lru_cache'd factories that hold a KBASE_AUTH_TOKEN reference.

    This list must be kept in sync with any @lru_cache client factories
    that pass the KBase token into the client they construct.
    """
    from berdl_notebook_utils.berdl_settings import get_settings
    from berdl_notebook_utils.clients import (
        get_governance_client,
        get_spark_cluster_client,
        get_task_service_client,
    )
    from berdl_notebook_utils.mcp.client import get_datalake_mcp_client

    get_settings.cache_clear()
    get_governance_client.cache_clear()
    get_spark_cluster_client.cache_clear()
    get_task_service_client.cache_clear()
    get_datalake_mcp_client.cache_clear()


def _sync_token():
    """Read token from session file and update env var if changed."""
    try:
        token = (Path.home() / TOKEN_CACHE_FILE).read_text().strip()
        if not token or len(token) < 32 or " " in token or "\n" in token:
            return
        if token != os.environ.get("KBASE_AUTH_TOKEN", ""):
            os.environ["KBASE_AUTH_TOKEN"] = token
            try:
                _clear_client_caches()
            except Exception:
                logger.debug("Failed to clear client caches", exc_info=True)
            logger.info("Kernel token updated from session file")
    except Exception:
        logger.debug("Token sync skipped: session file not available")


def _token_sync_loop():
    """Background loop that syncs the token periodically."""
    _sync_token()
    while True:
        time.sleep(TOKEN_SYNC_INTERVAL_SECONDS)
        _sync_token()


def start_token_sync():
    """Start the background token sync daemon thread."""
    try:
        thread = threading.Thread(target=_token_sync_loop, daemon=True, name="berdl-token-sync")
        thread.start()
        logger.info(f"Background token sync started (interval: {TOKEN_SYNC_INTERVAL_SECONDS}s)")
    except Exception as e:
        logger.warning(f"Failed to start token sync thread: {e}")


# Start on import
start_token_sync()
