"""
Refresh credentials and Spark environment.

Provides a single function to rotate the user's S3 + Polaris credentials,
re-fetch Polaris catalog metadata, restart the Spark Connect server, and
stop any existing Spark session — ensuring get_spark_session() works
afterward.
"""

import logging

from pyspark.sql import SparkSession

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.governance.operations import (
    get_polaris_catalog_info,
    rotate_credentials,
)
from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

logger = logging.getLogger("berdl.refresh")


def refresh_spark_environment() -> dict:
    """Re-provision credentials and restart Spark.

    Steps performed:
        1. Clear the in-memory ``get_settings()`` LRU cache
        2. Rotate the unified credential bundle via MMS — generates new
           S3 IAM secret AND new Polaris OAuth secret in one call;
           updates ``S3_ACCESS_KEY``, ``S3_SECRET_KEY``,
           ``POLARIS_CREDENTIAL`` env vars
        3. Re-fetch Polaris catalog metadata via the read-only
           effective-access endpoint (sets
           ``POLARIS_PERSONAL_CATALOG`` / ``POLARIS_TENANT_CATALOGS``)
        4. Clear settings cache again so downstream code sees fresh env vars
        5. Stop any existing Spark session
        6. Restart the Spark Connect server with regenerated spark-defaults.conf

    Returns:
        dict with keys ``credentials``, ``polaris_catalog``,
        ``spark_connect``, ``spark_session_stopped`` summarising what
        happened.
    """
    result: dict = {}

    # 1. Clear in-memory settings cache
    get_settings.cache_clear()

    # 2. Rotate the unified credential bundle (S3 + Polaris in one call)
    try:
        creds = rotate_credentials()
        result["credentials"] = {"status": "ok", "username": creds.username}
        logger.info("Credentials rotated for user: %s", creds.username)
    except Exception as exc:
        result["credentials"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to rotate credentials: %s", exc)

    # 3. Re-fetch Polaris catalog metadata (no credential side effect)
    try:
        catalog_info = get_polaris_catalog_info()
        if catalog_info:
            result["polaris_catalog"] = {
                "status": "ok",
                "personal_catalog": catalog_info["personal_catalog"],
                "tenant_catalogs": catalog_info.get("tenant_catalogs", []),
            }
            logger.info(
                "Polaris catalog metadata refreshed for catalog: %s",
                catalog_info["personal_catalog"],
            )
        else:
            result["polaris_catalog"] = {
                "status": "skipped",
                "reason": "Polaris not configured",
            }
            logger.info("Polaris not configured, skipping catalog metadata refresh")
    except Exception as exc:
        result["polaris_catalog"] = {"status": "error", "error": str(exc)}
        logger.warning("Failed to refresh Polaris catalog metadata: %s", exc)

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
