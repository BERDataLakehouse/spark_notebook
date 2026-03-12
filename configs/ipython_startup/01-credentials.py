"""
Initialize MinIO and Polaris credentials.

This runs after 00-notebookutils.py loads all the imports, so get_minio_credentials
is already available in the global namespace.
"""

# Setup logging
import logging

logger = logging.getLogger("berdl.startup")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- MinIO Credentials ---
try:
    # Set MinIO credentials to environment - also creates user if they don't exist
    credentials = get_minio_credentials()  # noqa: F821
    logger.info(f"✅ MinIO credentials set for user: {credentials.username}")

except Exception as e:
    import warnings

    warnings.warn(f"Failed to set MinIO credentials: {str(e)}", UserWarning)
    logger.error(f"❌ Failed to set MinIO credentials: {str(e)}")
    credentials = None

# --- Polaris Credentials ---
try:
    polaris_creds = get_polaris_credentials()  # noqa: F821
    if polaris_creds:
        logger.info(f"✅ Polaris credentials set for catalog: {polaris_creds['personal_catalog']}")
        if polaris_creds["tenant_catalogs"]:
            logger.info(f"   Tenant catalogs: {', '.join(polaris_creds['tenant_catalogs'])}")
        # Clear the settings cache so downstream code (e.g., Spark Connect server startup)
        # picks up the POLARIS_CREDENTIAL, POLARIS_PERSONAL_CATALOG, and
        # POLARIS_TENANT_CATALOGS env vars that get_polaris_credentials() just set.
        get_settings.cache_clear()  # noqa: F821
    else:
        logger.info("ℹ️  Polaris not configured, skipping Polaris credential setup")

except Exception as e:
    import warnings

    warnings.warn(f"Failed to set Polaris credentials: {str(e)}", UserWarning)
    logger.warning(f"⚠️  Failed to set Polaris credentials: {str(e)}")
    polaris_creds = None
