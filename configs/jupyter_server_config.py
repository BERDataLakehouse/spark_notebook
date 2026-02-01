import os
import logging
from hybridcontents import HybridContentsManager
from s3contents import S3ContentsManager
from jupyter_server.services.contents.largefilemanager import LargeFileManager
import json
from pathlib import Path

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
    """Extract MinIO configuration from credentials file or environment."""

    # Default values
    endpoint = os.environ.get("MINIO_ENDPOINT_URL", "minio:9002")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minio")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minio123")
    use_ssl = os.environ.get("MINIO_SECURE", "false").lower() == "true"

    # Try reading from credential file
    try:
        username = os.environ.get("NB_USER", "jovyan")
        cred_path = Path(f"/home/{username}/.berdl_minio_credentials")
        if cred_path.exists():
            data = json.loads(cred_path.read_text())
            # File format usually has keys: MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE
            if "MINIO_ENDPOINT_URL" in data:
                endpoint = data["MINIO_ENDPOINT_URL"]
            if "MINIO_ACCESS_KEY" in data:
                access_key = data["MINIO_ACCESS_KEY"]
            if "MINIO_SECRET_KEY" in data:
                secret_key = data["MINIO_SECRET_KEY"]
            if "MINIO_SECURE" in data:
                val = data["MINIO_SECURE"]
                if isinstance(val, bool):
                    use_ssl = val
                else:
                    use_ssl = str(val).lower() == "true"
            logger.info(f"Loaded MinIO credentials from {cred_path}")
    except Exception as e:
        logger.warning(f"Failed to read credential file: {e}")

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
    sources[f"my-files"] = {"bucket": "cdm-lake", "prefix": f"users-general-warehouse/{username}"}
    sources[f"my-sql"] = {"bucket": "cdm-lake", "prefix": f"users-sql-warehouse/{username}"}

    try:
        from berdl_notebook_utils.minio_governance import get_my_groups, get_my_workspace

        workspace = get_my_workspace()
        # Update username if different from workspace (unlikely but good for consistency)
        if workspace.username:
            username = workspace.username
            # Update defaults with confirmed username
            sources[f"my-files"] = {"bucket": "cdm-lake", "prefix": f"users-general-warehouse/{username}"}
            sources[f"my-sql"] = {"bucket": "cdm-lake", "prefix": f"users-sql-warehouse/{username}"}

        # 2. Group/Tenant Files
        groups_response = get_my_groups()
        user_groups = groups_response.groups or []

        unique_tenants = set()
        for group in user_groups:
            if group.endswith("ro"):
                unique_tenants.add(group[:-2])
            else:
                unique_tenants.add(group)

        for tenant in sorted(unique_tenants):
            sources[f"{tenant}-files"] = {"bucket": "cdm-lake", "prefix": f"tenant-general-warehouse/{tenant}"}
            sources[f"{tenant}-sql"] = {"bucket": "cdm-lake", "prefix": f"tenant-sql-warehouse/{tenant}"}

    except Exception as e:
        logger.error(f"Failed to resolve governance paths: {e}")
        # Fallback defaults are already set above

    return sources


# --- Main Configuration Logic ---

# 1. Local Manager (Root)
# We map the root directory to the user's home
username = os.environ.get("NB_USER", "jovyan")
c.HybridContentsManager.manager_classes = {
    "": LargeFileManager,
}
c.HybridContentsManager.manager_kwargs = {
    "": {"root_dir": f"/home/{username}"},
}

# 2. MinIO Managers
endpoint_url, access_key, secret_key, use_ssl = get_minio_config()
governance_paths = get_user_governance_paths()

for name, info in governance_paths.items():
    # Add to manager classes
    c.HybridContentsManager.manager_classes[name] = S3ContentsManager

    # Add config
    c.HybridContentsManager.manager_kwargs[name] = {
        "access_key_id": access_key,
        "secret_access_key": secret_key,
        "endpoint_url": endpoint_url,
        "bucket": info["bucket"],
        "prefix": info["prefix"],
        "signature_version": "s3v4",
        "region_name": "us-east-1",  # Often required arg even for MinIO
        # Pass additional arguments to s3fs via s3contents
        "s3fs_additional_kwargs": {
            "use_ssl": use_ssl,
        },
    }

logger.info(f"✅ s3contents configured with {len(governance_paths)} paths: {list(governance_paths.keys())}")
