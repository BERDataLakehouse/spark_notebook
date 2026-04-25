"""
Refresh credentials and Spark environment.

Provides a single function to rotate MinIO credentials, re-fetch MMS-backed
Polaris credentials, restart the Spark Connect server, and stop any existing
Spark session — ensuring get_spark_session() works afterward.
"""

import logging

from pyspark.sql import SparkSession

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.minio_governance.operations import (
    rotate_minio_credentials,
    get_polaris_credentials,
)
from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

logger = logging.getLogger("berdl.refresh")


def refresh_spark_environment() -> dict:
    """Re-provision credentials and restart Spark.

    Steps performed:
        1. Clear the in-memory ``get_settings()`` LRU cache
        2. Rotate MinIO credentials via MMS (generates new secret key, updates env vars)
        3. Re-fetch MMS-backed Polaris credentials (sets POLARIS_CREDENTIAL and catalog env vars)
        4. Clear settings cache again so downstream code sees fresh env vars
        5. Stop any existing Spark session
        6. Restart the Spark Connect server with regenerated spark-defaults.conf

    Returns:
        dict with keys ``minio``, ``polaris``, ``spark_connect``,
        ``spark_session_stopped`` summarising what happened.
    """
    result: dict = {}

    # 1. Clear in-memory settings cache
    get_settings.cache_clear()

    # 2. Rotate MinIO credentials (generates new secret key)
    try:
        minio_creds = rotate_minio_credentials()
        result["minio"] = {"status": "ok", "username": minio_creds.username}
        logger.info("MinIO credentials rotated for user: %s", minio_creds.username)
    except Exception as exc:
        result["minio"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to rotate MinIO credentials: %s", exc)

    # 3. Re-fetch MMS-backed Polaris credentials
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

    # 4. Clear settings cache again so get_settings() picks up new env vars
    get_settings.cache_clear()

    # 5. Stop existing Spark session
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()
        result["spark_session_stopped"] = True
        logger.info("Stopped existing Spark session")
    else:
        result["spark_session_stopped"] = False

    # 6. Restart Spark Connect server with fresh config
    try:
        sc_result = start_spark_connect_server(force_restart=True)
        result["spark_connect"] = sc_result
        logger.info("Spark Connect server restarted: %s", sc_result.get("status", "unknown"))
    except Exception as exc:
        result["spark_connect"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to restart Spark Connect server: %s", exc)

    return result
