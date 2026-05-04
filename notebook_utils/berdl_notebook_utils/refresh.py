"""
Refresh credentials and Spark + Trino runtimes.

Provides a single function to rotate the user's S3 + Polaris credentials,
re-fetch Polaris catalog metadata, restart the Spark Connect server, stop
any existing Spark session, AND re-create the Trino dynamic catalogs so
both engines pick up the rotated credentials in one call.
"""

import logging

from pyspark.sql import SparkSession

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.governance.operations import (
    get_polaris_catalog_info,
    rotate_credentials,
)
from berdl_notebook_utils.setup_trino_session import get_trino_connection
from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

logger = logging.getLogger("berdl.refresh")


def refresh_spark_environment() -> dict:
    """Re-provision credentials and refresh Spark + Trino.

    Steps performed:
        1. Clear the in-memory ``get_settings()`` LRU cache (defensive,
           in case env vars were mutated outside this function before
           the call).
        2. Rotate the unified credential bundle via MMS — generates new
           S3 IAM secret AND new Polaris OAuth secret in one call;
           updates ``S3_ACCESS_KEY``, ``S3_SECRET_KEY``,
           ``POLARIS_CREDENTIAL`` env vars.  ``rotate_credentials`` calls
           ``get_settings.cache_clear()`` internally after writing the
           env vars, so subsequent ``get_settings()`` reads are fresh
           without the caller re-clearing.
        3. Re-fetch Polaris catalog metadata via the read-only
           effective-access endpoint (sets
           ``POLARIS_PERSONAL_CATALOG`` / ``POLARIS_TENANT_CATALOGS``).
           ``get_polaris_catalog_info`` also clears the cache itself.
        4. Stop any existing Spark session
        5. Restart the Spark Connect server with regenerated
           spark-defaults.conf (this calls ``get_settings()`` and so
           re-populates the cache with the rotated values)
        6. Re-open a Trino connection so the dynamic Polaris catalogs
           are dropped and re-created with the rotated
           ``POLARIS_CREDENTIAL``.  Skipped when Polaris is not
           configured.

    Returns:
        dict with keys ``credentials``, ``polaris_catalog``,
        ``spark_connect``, ``spark_session_stopped``, ``trino_catalogs``
        summarising what happened.
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

    # 6. Re-open a Trino connection so the per-user Polaris dynamic
    #    catalogs are dropped and recreated with the rotated
    #    POLARIS_CREDENTIAL.
    #
    #    Without this step, Trino's coordinator keeps the previously-
    #    cached oauth2.credential (= the pre-rotation client_secret)
    #    baked into the long-lived ``my`` / tenant catalogs. Even with
    #    ``token-refresh-enabled=true`` the refresh request itself uses
    #    the (now stale) cached secret and is rejected by Polaris with
    #    ``unauthorized_client``, surfacing as
    #    ``ICEBERG_CATALOG_ERROR: Failed to list namespaces`` on every
    #    subsequent query.  ``_create_polaris_catalogs`` calls
    #    ``_create_dynamic_catalog(..., force=True)`` so the existing
    #    catalogs are dropped before recreation.
    settings = get_settings()
    if not settings.POLARIS_CATALOG_URI:
        result["trino_catalogs"] = {"status": "skipped", "reason": "Polaris not configured"}
    else:
        try:
            conn = get_trino_connection()
            try:
                conn.close()
            except Exception:  # noqa: BLE001 — closing failure is non-fatal
                pass
            result["trino_catalogs"] = {"status": "ok"}
            logger.info("Trino dynamic Polaris catalogs refreshed")
        except Exception as exc:
            result["trino_catalogs"] = {"status": "error", "error": str(exc)}
            logger.warning("Failed to refresh Trino dynamic catalogs: %s", exc)

    return result
