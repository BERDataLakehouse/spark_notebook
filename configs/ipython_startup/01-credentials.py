"""
Initialize unified S3 + Polaris credentials and Polaris catalog metadata.

This runs after 00-notebookutils.py loads all the imports, so
``get_credentials`` and ``get_polaris_catalog_info`` are already
available in the global namespace.

Two MMS round-trips:
  1. ``get_credentials()`` — fetches the unified credential bundle and
     populates ``S3_ACCESS_KEY``, ``S3_SECRET_KEY``, ``POLARIS_CREDENTIAL``.
  2. ``get_polaris_catalog_info()`` — read-only catalog discovery via
     ``GET /polaris/effective-access/me``; populates
     ``POLARIS_PERSONAL_CATALOG`` and ``POLARIS_TENANT_CATALOGS``.
"""

# Setup logging
import logging
import warnings

logger = logging.getLogger("berdl.startup")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Unified S3 + Polaris credentials ---
# One MMS call populates S3_ACCESS_KEY, S3_SECRET_KEY, POLARIS_CREDENTIAL.
# MMS self-bootstraps the underlying identities (MinIO user + Polaris
# principal + personal catalog + role bindings) on cache miss.
try:
    credentials = get_credentials()  # noqa: F821
    logger.info(f"✅ Credentials set for user: {credentials.username}")

except Exception as e:
    warnings.warn(f"Failed to set credentials: {str(e)}", UserWarning)
    logger.error(f"❌ Failed to set credentials: {str(e)}")
    credentials = None

# --- Polaris catalog metadata ---
# Separate read-only call — fetches personal_catalog name + tenant catalog
# list. No provisioning side effect (uses GET /polaris/effective-access/me).
try:
    catalog_info = get_polaris_catalog_info()  # noqa: F821
    if catalog_info:
        logger.info(f"✅ Polaris catalog metadata: {catalog_info['personal_catalog']}")
        if catalog_info["tenant_catalogs"]:
            logger.info(f"   Tenant catalogs: {', '.join(catalog_info['tenant_catalogs'])}")
        # Clear the settings cache so downstream code (e.g., Spark Connect
        # server startup) picks up POLARIS_PERSONAL_CATALOG and
        # POLARIS_TENANT_CATALOGS that get_polaris_catalog_info() just set.
        get_settings.cache_clear()  # noqa: F821
    else:
        logger.info("ℹ️  Polaris not configured, skipping catalog metadata fetch")

except Exception as e:
    warnings.warn(f"Failed to fetch Polaris catalog metadata: {str(e)}", UserWarning)
    logger.warning(f"⚠️  Failed to fetch Polaris catalog metadata: {str(e)}")
    catalog_info = None
