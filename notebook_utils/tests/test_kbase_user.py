"""Tests for berdl_notebook_utils.kbase_user."""

from unittest.mock import patch

import httpx
import pytest

from berdl_notebook_utils import kbase_user
from berdl_notebook_utils.kbase_user import (
    ORCID_ENV_VAR,
    extract_orcid,
    fetch_user_profile,
    sync_orcid_to_env,
)

# A representative /api/V2/me response used across tests.
SAMPLE_PROFILE_WITH_ORCID = {
    "user": "tgu2",
    "display": "Tianhao Gu",
    "email": "tgu@anl.gov",
    "idents": [
        {
            "provusername": "tgu2@globusid.org",
            "provider": "Globus",
            "id": "0775d642c4bec0b1d7eb62cb9ca6c898",
        },
        {
            "provusername": "0009-0004-8221-7736",
            "provider": "OrcID",
            "id": "1e7dfb1d1352cadd0e967293f9894b60",
        },
    ],
}

SAMPLE_PROFILE_WITHOUT_ORCID = {
    "user": "globusonly",
    "idents": [
        {
            "provusername": "globusonly@globusid.org",
            "provider": "Globus",
            "id": "abc",
        },
    ],
}


@pytest.fixture(autouse=True)
def _clear_orcid_env(monkeypatch):
    """Ensure each test starts with the ORCID env var unset."""
    monkeypatch.delenv(ORCID_ENV_VAR, raising=False)
    yield


# ---------------------------------------------------------------------------
# extract_orcid
# ---------------------------------------------------------------------------


class TestExtractOrcid:
    def test_returns_orcid_when_present(self):
        assert extract_orcid(SAMPLE_PROFILE_WITH_ORCID) == "0009-0004-8221-7736"

    def test_returns_none_when_no_orcid_provider(self):
        assert extract_orcid(SAMPLE_PROFILE_WITHOUT_ORCID) is None

    def test_returns_none_when_no_idents_field(self):
        assert extract_orcid({"user": "x"}) is None

    def test_returns_none_when_idents_is_empty(self):
        assert extract_orcid({"user": "x", "idents": []}) is None

    def test_returns_none_when_idents_is_none(self):
        # Defensive: KBase auth has been observed to return null fields.
        assert extract_orcid({"user": "x", "idents": None}) is None

    def test_returns_none_when_orcid_provusername_missing(self):
        assert (
            extract_orcid({"idents": [{"provider": "OrcID"}]}) is None
        )

    def test_provider_match_is_case_sensitive(self):
        # KBase auth uses literal "OrcID" — we do not normalize.
        assert (
            extract_orcid({"idents": [{"provider": "orcid", "provusername": "x"}]})
            is None
        )

    def test_returns_first_orcid_when_multiple(self):
        profile = {
            "idents": [
                {"provider": "OrcID", "provusername": "0000-0001-1111-1111"},
                {"provider": "OrcID", "provusername": "0000-0002-2222-2222"},
            ]
        }
        assert extract_orcid(profile) == "0000-0001-1111-1111"


# ---------------------------------------------------------------------------
# fetch_user_profile
# ---------------------------------------------------------------------------


def _response(url: str, status: int = 200, **kwargs) -> httpx.Response:
    """Build an httpx.Response wired to a Request so raise_for_status works."""
    return httpx.Response(status, request=httpx.Request("GET", url), **kwargs)


class TestFetchUserProfile:
    def test_calls_v2_me_with_token_header(self):
        captured = {}

        def fake_get(url, headers, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _response(url, json=SAMPLE_PROFILE_WITH_ORCID)

        with patch.object(kbase_user.httpx, "get", side_effect=fake_get):
            result = fetch_user_profile(
                "https://ci.kbase.us/services/auth/", "TOK", timeout=5.0
            )

        assert result == SAMPLE_PROFILE_WITH_ORCID
        assert captured["url"] == "https://ci.kbase.us/services/auth/api/V2/me"
        assert captured["headers"] == {"Authorization": "TOK"}
        assert captured["timeout"] == 5.0

    def test_strips_trailing_slash_on_auth_url(self):
        captured = {}

        def fake_get(url, headers, timeout):
            captured["url"] = url
            return _response(url, json={})

        with patch.object(kbase_user.httpx, "get", side_effect=fake_get):
            fetch_user_profile("https://example/services/auth///", "T")

        assert captured["url"] == "https://example/services/auth/api/V2/me"

    def test_raises_on_http_error(self):
        def fake_get(url, headers, timeout):
            return _response(url, status=401, text="unauthorized")

        with patch.object(kbase_user.httpx, "get", side_effect=fake_get):
            with pytest.raises(httpx.HTTPStatusError):
                fetch_user_profile("https://example/", "BAD")


# ---------------------------------------------------------------------------
# sync_orcid_to_env
# ---------------------------------------------------------------------------


class TestSyncOrcidToEnv:
    def test_sets_env_var_when_orcid_present(self, monkeypatch):
        monkeypatch.setenv("KBASE_AUTH_URL", "https://ci.kbase.us/services/auth/")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "TOK")
        with patch.object(
            kbase_user, "fetch_user_profile", return_value=SAMPLE_PROFILE_WITH_ORCID
        ):
            result = sync_orcid_to_env()

        assert result == "0009-0004-8221-7736"
        import os

        assert os.environ[ORCID_ENV_VAR] == "0009-0004-8221-7736"

    def test_returns_none_and_leaves_env_unset_when_no_orcid(self, monkeypatch):
        monkeypatch.setenv("KBASE_AUTH_URL", "https://ci.kbase.us/services/auth/")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "TOK")
        with patch.object(
            kbase_user, "fetch_user_profile", return_value=SAMPLE_PROFILE_WITHOUT_ORCID
        ):
            result = sync_orcid_to_env()

        import os

        assert result is None
        assert ORCID_ENV_VAR not in os.environ

    def test_noop_when_auth_url_missing(self, monkeypatch):
        monkeypatch.delenv("KBASE_AUTH_URL", raising=False)
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "TOK")
        with patch.object(kbase_user, "fetch_user_profile") as fetch:
            result = sync_orcid_to_env()

        assert result is None
        fetch.assert_not_called()

    def test_noop_when_token_missing(self, monkeypatch):
        monkeypatch.setenv("KBASE_AUTH_URL", "https://example/")
        monkeypatch.delenv("KBASE_AUTH_TOKEN", raising=False)
        with patch.object(kbase_user, "fetch_user_profile") as fetch:
            result = sync_orcid_to_env()

        assert result is None
        fetch.assert_not_called()

    def test_swallows_http_errors(self, monkeypatch):
        monkeypatch.setenv("KBASE_AUTH_URL", "https://example/")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "TOK")
        with patch.object(
            kbase_user,
            "fetch_user_profile",
            side_effect=httpx.ConnectError("boom"),
        ):
            result = sync_orcid_to_env()

        import os

        assert result is None
        assert ORCID_ENV_VAR not in os.environ

    def test_explicit_args_override_env(self, monkeypatch):
        monkeypatch.setenv("KBASE_AUTH_URL", "https://wrong/")
        monkeypatch.setenv("KBASE_AUTH_TOKEN", "wrong")
        with patch.object(
            kbase_user, "fetch_user_profile", return_value=SAMPLE_PROFILE_WITH_ORCID
        ) as fetch:
            sync_orcid_to_env(auth_url="https://right/", token="right")

        fetch.assert_called_once_with("https://right/", "right")
