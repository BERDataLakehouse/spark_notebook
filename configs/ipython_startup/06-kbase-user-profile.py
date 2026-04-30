"""
Sync KBase user-profile fields into the kernel environment.
==========================================================

Runs once at kernel startup. Calls KBase auth ``/api/V2/me`` and exports
identity fields that are useful to user notebooks but not provided by the
spawner:

* ``ORCID`` — the user's linked ORCID identifier, when present.

Best-effort: missing token / auth URL or transient HTTP errors are logged
and the script exits cleanly without raising.
"""

import logging

from berdl_notebook_utils.berdl_settings import get_settings
from berdl_notebook_utils.kbase_user import sync_orcid_to_env

logger = logging.getLogger("berdl.startup")

try:
    orcid = sync_orcid_to_env()
    if orcid:
        # Drop any cached settings so subsequent get_settings() reflects ORCID
        get_settings.cache_clear()
        logger.info(f"✅ ORCID={orcid}")
    else:
        logger.info("ℹ️  ORCID not set (no linked ORCID identity or auth not configured)")
except Exception as e:
    logger.warning(f"Failed to sync KBase user profile: {e}")
