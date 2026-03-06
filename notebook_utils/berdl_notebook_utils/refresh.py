"""
Refresh credentials and Spark environment.

Provides a single function to clear all credential caches, re-provision
MinIO and Polaris credentials, restart the Spark Connect server, and stop
any existing Spark session — ensuring get_spark_session() works afterward.
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.minio_governance.operations import (
    CREDENTIALS_CACHE_FILE,
    POLARIS_CREDENTIALS_CACHE_FILE,
    get_minio_credentials,
    get_polaris_credentials,
)
from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

logger = logging.getLogger("berdl.refresh")


def _remove_cache_file(path: Path) -> bool:
    """Remove a cache file and its .lock companion. Returns True if the main file existed."""
    existed = False
    for p in (path, path.with_suffix(path.suffix + ".lock")):
        try:
            if p.exists():
                p.unlink()
                if p == path:
                    existed = True
        except OSError:
            pass
    return existed


def refresh_spark_environment() -> dict:
    """Clear all credential caches, re-provision credentials, and restart Spark.

    Steps performed:
        1. Delete MinIO and Polaris credential cache files
        2. Clear the in-memory ``get_settings()`` LRU cache
        3. Re-fetch MinIO credentials (sets MINIO_ACCESS_KEY/SECRET_KEY env vars)
        4. Re-fetch Polaris credentials (sets POLARIS_CREDENTIAL and catalog env vars)
        5. Clear settings cache again so downstream code sees fresh env vars
        6. Stop any existing Spark session
        7. Restart the Spark Connect server with regenerated spark-defaults.conf

    Returns:
        dict with keys ``minio``, ``polaris``, ``spark_connect``,
        ``spark_session_stopped`` summarising what happened.
    """
    home = Path.home()
    result: dict = {}

    # 1. Delete credential cache files
    minio_removed = _remove_cache_file(home / CREDENTIALS_CACHE_FILE)
    polaris_removed = _remove_cache_file(home / POLARIS_CREDENTIALS_CACHE_FILE)
    logger.info(
        "Cleared credential caches (minio=%s, polaris=%s)",
        minio_removed,
        polaris_removed,
    )

    # 2. Clear in-memory settings cache
    get_settings.cache_clear()

    # 3. Re-fetch MinIO credentials
    try:
        minio_creds = get_minio_credentials()
        result["minio"] = {"status": "ok", "username": minio_creds.username}
        logger.info("MinIO credentials refreshed for user: %s", minio_creds.username)
    except Exception as exc:
        result["minio"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to refresh MinIO credentials: %s", exc)

    # 4. Re-fetch Polaris credentials
    try:
        polaris_creds = get_polaris_credentials()
        if polaris_creds:
            result["polaris"] = {
                "status": "ok",
                "personal_catalog": polaris_creds["personal_catalog"],
                "tenant_catalogs": polaris_creds.get("tenant_catalogs", []),
            }
            logger.info("Polaris credentials refreshed for catalog: %s", polaris_creds["personal_catalog"])
        else:
            result["polaris"] = {"status": "skipped", "reason": "Polaris not configured"}
            logger.info("Polaris not configured, skipping credential refresh")
    except Exception as exc:
        result["polaris"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to refresh Polaris credentials: %s", exc)

    # 5. Clear settings cache again so get_settings() picks up new env vars
    get_settings.cache_clear()

    # 6. Stop existing Spark session
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()
        result["spark_session_stopped"] = True
        logger.info("Stopped existing Spark session")
    else:
        result["spark_session_stopped"] = False

    # 7. Restart Spark Connect server with fresh config
    try:
        sc_result = start_spark_connect_server(force_restart=True)
        result["spark_connect"] = sc_result
        logger.info("Spark Connect server restarted: %s", sc_result.get("status", "unknown"))
    except Exception as exc:
        result["spark_connect"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to restart Spark Connect server: %s", exc)

    return result
