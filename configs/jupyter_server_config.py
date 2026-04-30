import os
import sys
import logging

# Add config directory to path for local imports
# Note: __file__ may not be defined when exec'd by traitlets config loader
sys.path.insert(0, "/etc/jupyter")

from berdl_notebook_utils.berdl_settings import get_settings
from hybridcontents import HybridContentsManager
from jupyter_server.services.contents.largefilemanager import LargeFileManager
from grouped_s3_contents import GroupedS3ContentsManager

from berdl_notebook_utils.kbase_user import sync_orcid_to_env

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("berdl.jupyter_config")

c = get_config()  # noqa: F821

# ------------------------------------------------------------------------------
# Server Connection Configuration
# ------------------------------------------------------------------------------
# Bind to all interfaces to allow access from outside the container
c.ServerApp.ip = "0.0.0.0"


# ------------------------------------------------------------------------------
# HybridContentsManager Configuration
# ------------------------------------------------------------------------------
# We use HybridContentsManager to mix local files and MinIO buckets.
c.ServerApp.contents_manager_class = HybridContentsManager

# Default local files settings
# This will be mapped to "local" or just root if we could, but HybridContents usually requires dict properties.
# We will map:
#   "" -> Local /home/{user} (This might be tricky with Hybrid, usually it names subfolders)
#   Actually, HybridContentsManager allows a "root" manager for ""?
#   No, it usually maps specific prefixes.
#   Common pattern:
#     "local": LargeFileManager(root_dir=...)
#     "s3": S3ContentsManager(...)
#
#   If we want the *root* of the file browser to correspond to the home dir, and s3 buckets as subfolders:
#   We can't easily do that with HybridContentsManager unless we map "" to local, which handles everything else?
#   Documentation says: "manager_classes" and "manager_kwargs" are dicts. Keys are directories.
#   "The key '' (empty string) corresponds to the root directory."


def get_minio_config():
    """Extract MinIO configuration, provisioning credentials via governance API if needed."""
    from berdl_notebook_utils.minio_governance import get_minio_credentials

    # Provision user + fetch credentials (checks cache first, calls API if needed,
    # sets MINIO_ACCESS_KEY/MINIO_SECRET_KEY env vars)
    credentials = get_minio_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key

    endpoint = os.environ.get("MINIO_ENDPOINT_URL")
    use_ssl = os.environ.get("MINIO_SECURE", "false").lower() == "true"

    if not endpoint:
        raise ValueError("MINIO_ENDPOINT_URL is required")

    if not endpoint.startswith(("http://", "https://")):
        protocol = "https://" if use_ssl else "http://"
        endpoint = f"{protocol}{endpoint}"

    return endpoint, access_key, secret_key, use_ssl


def get_user_governance_paths():
    """Resolve user paths using governance utils."""
    sources = {}

    # Defaults based on NB_USER
    username = os.environ.get("NB_USER", "jovyan")

    # 1. User Personal Files
    sources["my-files"] = {"bucket": "cdm-lake", "prefix": f"users-general-warehouse/{username}"}
    sources["my-sql"] = {"bucket": "cdm-lake", "prefix": f"users-sql-warehouse/{username}"}

    try:
        from berdl_notebook_utils.minio_governance import get_my_groups, get_my_workspace

        workspace = get_my_workspace()
        # Update username if different from workspace (unlikely but good for consistency)
        if workspace.username:
            username = workspace.username
            # Update defaults with confirmed username
            sources["my-files"] = {"bucket": "cdm-lake", "prefix": f"users-general-warehouse/{username}"}
            sources["my-sql"] = {"bucket": "cdm-lake", "prefix": f"users-sql-warehouse/{username}"}

        # 2. Group/Tenant Files
        groups_response = get_my_groups()
        user_groups = set(groups_response.groups or [])

        # Identify all potential tenants from group names (stripping ro suffix)
        potential_tenants = set()
        for group in user_groups:
            # User example: "kbasero" -> tenant "kbase"
            if group.endswith("ro"):
                potential_tenants.add(group[:-2])
            else:
                potential_tenants.add(group)

        for tenant in sorted(potential_tenants):
            # Check explicit membership
            has_rw = tenant in user_groups
            has_ro = f"{tenant}ro" in user_groups

            # Decide what to mount
            if has_rw:
                # RW Access - Mount normally
                sources[f"{tenant}-files"] = {"bucket": "cdm-lake", "prefix": f"tenant-general-warehouse/{tenant}"}
                sources[f"{tenant}-sql"] = {"bucket": "cdm-lake", "prefix": f"tenant-sql-warehouse/{tenant}"}
            elif has_ro:
                # RO Access - Mount with suffix and read_only flag
                sources[f"{tenant}-files-ro"] = {
                    "bucket": "cdm-lake",
                    "prefix": f"tenant-general-warehouse/{tenant}",
                    "read_only": True,
                }
                sources[f"{tenant}-sql-ro"] = {
                    "bucket": "cdm-lake",
                    "prefix": f"tenant-sql-warehouse/{tenant}",
                    "read_only": True,
                }
            # If neither (e.g. was a partial match or unrelated group), skip

    except Exception as e:
        logger.error(f"Failed to resolve governance paths: {e}")
        # Fallback defaults are already set above

    return sources


def provision_polaris():
    """Provision Polaris credentials at server startup and set env vars.

    Called once at Jupyter Server startup so credentials are available
    before any notebook kernel opens. Subsequent calls from IPython startup
    scripts will hit the file cache and return immediately.
    """
    try:
        from berdl_notebook_utils.minio_governance import get_polaris_credentials

        polaris_creds = get_polaris_credentials()
        if polaris_creds:
            logger.info(f"Polaris credentials provisioned for catalog: {polaris_creds['personal_catalog']}")
            if polaris_creds["tenant_catalogs"]:
                logger.info(f"   Tenant catalogs: {', '.join(polaris_creds['tenant_catalogs'])}")
        else:
            logger.info("Polaris not configured, skipping Polaris credential provisioning")
    except Exception as e:
        logger.error(f"Failed to provision Polaris credentials: {e}")


def provision_kbase_user_profile():
    """Sync KBase user-profile fields (currently ORCID) to the server env.

    Called once at Jupyter Server startup so values like ORCID are inherited
    by all kernels spawned from this server. Failures are logged and
    swallowed — they must not prevent the server from starting.
    """
    try:
        orcid = sync_orcid_to_env()
        if orcid:
            logger.info(f"ORCID provisioned for current user: {orcid}")
        else:
            logger.info("ORCID not set (no linked ORCID or KBASE_AUTH_URL not configured)")
    except Exception as e:
        logger.warning(f"Failed to sync KBase user profile: {e}")


def start_spark_connect():
    """Start Spark Connect server at Jupyter Server startup.

    Runs in a background thread so it doesn't block the server from accepting
    connections. Idempotent: reuses existing process if already running.
    """
    import threading

    def _start():
        try:
            from berdl_notebook_utils.spark.connect_server import start_spark_connect_server

            server_info = start_spark_connect_server()
            logger.info(f"Spark Connect server ready at {server_info['url']}")
        except Exception as e:
            logger.error(f"Failed to start Spark Connect server: {e}")

    t = threading.Thread(target=_start, name="spark-connect-startup", daemon=True)
    t.start()


# --- Main Configuration Logic ---

# 1. Local Manager (Root)
# We map the root directory to the user's home
username = os.environ.get("NB_USER", "jovyan")

# 2. Get MinIO configuration (also provisions/caches the user in MinIO)
endpoint_url, access_key, secret_key, use_ssl = get_minio_config()
governance_paths = get_user_governance_paths()

# 3. Provision Polaris credentials — MUST be before Spark Connect so that
#    POLARIS_CREDENTIAL env vars are set when generating spark-defaults.conf
provision_polaris()

# Clear the settings cache so start_spark_connect picks up the new
# POLARIS_CREDENTIAL/POLARIS_PERSONAL_CATALOG/POLARIS_TENANT_CATALOGS env vars
# that provision_polaris() just set. Without this, the lru_cache returns the
# stale settings object captured before Polaris provisioning ran.
get_settings.cache_clear()

# 4. Provision KBase user-profile fields (ORCID) so spawned kernels inherit them
provision_kbase_user_profile()

# 5. Start Spark Connect server in background (non-blocking)
start_spark_connect()

# 6. Configure HybridContentsManager
# - Root ("") -> Local filesystem
# - "datalake_minio" -> GroupedS3ContentsManager with all S3 paths as subdirectories
c.HybridContentsManager.manager_classes = {
    "": LargeFileManager,
    "lakehouse_minio": GroupedS3ContentsManager,
}

c.HybridContentsManager.manager_kwargs = {
    "": {"root_dir": f"/home/{username}"},
    "lakehouse_minio": {
        "endpoint_url": endpoint_url,
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "region_name": "us-east-1",
        "signature_version": "s3v4",
        "s3fs_additional_kwargs": {"use_ssl": use_ssl},
        # Each entry becomes a subdirectory under "lakehouse_minio/"
        "managers": governance_paths,
        # Re-check group membership dynamically so the file browser
        # reflects tenant changes without requiring logout/login
        "governance_path_resolver": get_user_governance_paths,
    },
}

# Configure FilesHandler to serve files from the correct root directory
# This is required when using HybridContentsManager for file downloads via /files/ route
c.ContentsManager.files_handler_params = {"path": f"/home/{username}"}

logger.info(f"✅ GroupedS3ContentsManager 'lakehouse_minio/' configured with: {list(governance_paths.keys())}")
