"""
Background Token Sync for Kernel Processes
==========================================

Starts a daemon thread that reads ~/.berdl_kbase_session every 30 seconds
and updates os.environ["KBASE_AUTH_TOKEN"] in this kernel process when it
changes.

When the token changes, all @kbase_token_dependent-registered caches are
invalidated so they rebuild with the fresh token on next use.
"""

import logging
import threading
import time

from berdl_notebook_utils.cache import sync_kbase_token_from_cache_file

logger = logging.getLogger(__name__)

TOKEN_SYNC_INTERVAL_SECONDS = 30


def _sync_token():
    """Read token from session file and update env var if changed."""
    try:
        if sync_kbase_token_from_cache_file():
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
