"""KBase auth user-profile helpers.

Fetches user-profile data from the KBase auth service ``/api/V2/me`` endpoint
and surfaces commonly-needed fields (e.g. the user's ORCID identifier) into
the kernel/process environment so notebooks can read them via ``os.environ``.

ORCID is exported as ``ORCID`` when the user has linked their KBase account
to ORCID. Calling this module is best-effort: any HTTP / parse error is
logged and the env var is left unchanged.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

#: Environment variable name set by :func:`sync_orcid_to_env`.
ORCID_ENV_VAR = "ORCID"

#: ``provider`` value used by the KBase auth service for ORCID-linked identities.
ORCID_PROVIDER = "OrcID"

#: Default HTTP timeout for /api/V2/me calls.
_DEFAULT_TIMEOUT_SECONDS = 10.0


def fetch_user_profile(
    auth_url: str,
    token: str,
    *,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Fetch ``/api/V2/me`` from a KBase auth service.

    Args:
        auth_url: KBase auth base URL (e.g. ``https://ci.kbase.us/services/auth/``).
            Trailing slash is optional.
        token: KBase auth token (sent as the ``Authorization`` header value).
        timeout: HTTP timeout in seconds.

    Returns:
        Parsed JSON response. The shape includes keys like ``user``, ``display``,
        ``email``, ``customroles`` and ``idents`` (a list of linked identity
        provider entries).

    Raises:
        httpx.HTTPError: On network failure or non-2xx response.
    """
    url = f"{auth_url.rstrip('/')}/api/V2/me"
    response = httpx.get(
        url,
        headers={"Authorization": token},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def extract_orcid(profile: dict[str, Any]) -> str | None:
    """Return the user's ORCID identifier from a profile dict, or ``None``.

    Looks for an entry in ``profile["idents"]`` whose ``provider`` equals
    :data:`ORCID_PROVIDER` and returns its ``provusername`` (the bare ORCID id
    in the form ``XXXX-XXXX-XXXX-XXXX``).
    """
    for ident in profile.get("idents") or []:
        if ident.get("provider") == ORCID_PROVIDER:
            orcid = ident.get("provusername")
            if orcid:
                return str(orcid)
    return None


def sync_orcid_to_env(
    auth_url: str | None = None,
    token: str | None = None,
) -> str | None:
    """Fetch the current user's ORCID and export it as ``ORCID``.

    Best-effort: any HTTP / parse error is logged at WARNING level and ``None``
    is returned; the env var is unchanged. When the user has no linked ORCID
    identity, ``None`` is also returned (and the env var is unchanged).

    Args:
        auth_url: KBase auth base URL. Defaults to ``$KBASE_AUTH_URL``.
        token: KBase auth token. Defaults to ``$KBASE_AUTH_TOKEN``.

    Returns:
        The ORCID identifier when found and exported, otherwise ``None``.
    """
    auth_url = auth_url or os.environ.get("KBASE_AUTH_URL")
    token = token or os.environ.get("KBASE_AUTH_TOKEN")
    if not auth_url or not token:
        logger.debug(
            "Skipping ORCID sync: KBASE_AUTH_URL and KBASE_AUTH_TOKEN must both be set"
        )
        return None

    try:
        profile = fetch_user_profile(auth_url, token)
    except Exception as exc:
        logger.warning("Failed to fetch KBase user profile from %s: %s", auth_url, exc)
        return None

    orcid = extract_orcid(profile)
    user = profile.get("user", "<unknown>")
    if orcid:
        os.environ[ORCID_ENV_VAR] = orcid
        logger.info("Set %s=%s for user %s", ORCID_ENV_VAR, orcid, user)
    else:
        logger.info("User %s has no linked ORCID identity; %s not set", user, ORCID_ENV_VAR)
    return orcid
