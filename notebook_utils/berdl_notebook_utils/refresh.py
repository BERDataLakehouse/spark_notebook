"""
Refresh credentials and Spark environment.

Provides a single function to re-provision MinIO credentials, restart
the Spark Connect server, and stop any existing Spark session — ensuring
get_spark_session() works afterward.
"""

import logging

from pyspark.sql import SparkSession

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.minio_governance.operations import rotate_minio_credentials
from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

logger = logging.getLogger("berdl.refresh")


def refresh_spark_environment() -> dict:
    """Re-provision credentials and restart Spark.

    Steps performed:
        1. Clear the in-memory ``get_settings()`` LRU cache
        2. Rotate MinIO credentials via MMS (generates new secret key, updates env vars)
        3. Clear settings cache again so downstream code sees fresh env vars
        4. Stop any existing Spark session
        5. Restart the Spark Connect server with regenerated spark-defaults.conf

    Returns:
        dict with keys ``minio``, ``spark_connect``,
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

    # 3. Clear settings cache again so get_settings() picks up new env vars
    get_settings.cache_clear()

    # 4. Stop existing Spark session
    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()
        result["spark_session_stopped"] = True
        logger.info("Stopped existing Spark session")
    else:
        result["spark_session_stopped"] = False

    # 5. Restart Spark Connect server with fresh config
    try:
        sc_result = start_spark_connect_server(force_restart=True)
        result["spark_connect"] = sc_result
        logger.info("Spark Connect server restarted: %s", sc_result.get("status", "unknown"))
    except Exception as exc:
        result["spark_connect"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to restart Spark Connect server: %s", exc)

    return result
