"""
Tests for minio_governance/tenant_management.py module.
"""

from unittest.mock import Mock, patch

import pytest
from governance_client.models import (
    ErrorResponse,
    TenantDetailResponse,
    TenantMemberResponse,
    TenantMetadataResponse,
    TenantStewardResponse,
    TenantSummaryResponse,
)

from berdl_notebook_utils.minio_governance.tenant_management import (
    _print_tenant,
    _val,
    add_tenant_member,
    assign_steward,
    get_my_steward_tenants,
    get_tenant_detail,
    get_tenant_members,
    get_tenant_stewards,
    list_tenants,
    remove_steward,
    remove_tenant_member,
    show_my_tenants,
    update_tenant_metadata,
)

MODULE = "berdl_notebook_utils.minio_governance.tenant_management"


# =============================================================================
# list_tenants
# =============================================================================


class TestListTenants:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_returns_list(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        summary = Mock(spec=TenantSummaryResponse)
        mock_api.sync.return_value = [summary]

        result = list_tenants()

        assert result == [summary]
        mock_api.sync.assert_called_once()

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="forbidden", error_type="auth")

        with pytest.raises(RuntimeError, match="forbidden"):
            list_tenants()

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_raises_on_none(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response"):
            list_tenants()

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_cache_hit_skips_api(self, mock_api, mock_client):
        """Second call returns cached result without hitting the API again."""
        mock_client.return_value = Mock()
        summary = Mock(spec=TenantSummaryResponse)
        mock_api.sync.return_value = [summary]

        result1 = list_tenants()
        result2 = list_tenants()

        assert result1 == result2 == [summary]
        mock_api.sync.assert_called_once()

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_force_refresh_bypasses_cache(self, mock_api, mock_client):
        """force_refresh=True always hits the API."""
        mock_client.return_value = Mock()
        first = Mock(spec=TenantSummaryResponse)
        second = Mock(spec=TenantSummaryResponse)
        mock_api.sync.side_effect = [[first], [second]]

        result1 = list_tenants()
        result2 = list_tenants(force_refresh=True)

        assert result1 == [first]
        assert result2 == [second]
        assert mock_api.sync.call_count == 2


# =============================================================================
# get_my_steward_tenants
# =============================================================================


class TestGetMyStewardTenants:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_filters_to_steward_only(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        steward_tenant = Mock(spec=TenantSummaryResponse, is_steward=True, tenant_name="mine")
        member_tenant = Mock(spec=TenantSummaryResponse, is_steward=False, tenant_name="other")
        mock_api.sync.return_value = [steward_tenant, member_tenant]

        result = get_my_steward_tenants()

        assert result == [steward_tenant]

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.list_tenants_tenants_get")
    def test_returns_empty_when_not_steward(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = [
            Mock(spec=TenantSummaryResponse, is_steward=False),
        ]

        result = get_my_steward_tenants()

        assert result == []


# =============================================================================
# get_tenant_detail
# =============================================================================


class TestGetTenantDetail:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_detail_tenants_tenant_name_get")
    def test_returns_detail(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        detail = Mock(spec=TenantDetailResponse)
        mock_api.sync.return_value = detail

        result = get_tenant_detail("kbase")

        assert result is detail
        mock_api.sync.assert_called_once_with(client=mock_client.return_value, tenant_name="kbase")

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_detail_tenants_tenant_name_get")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="not found", error_type="not_found")

        with pytest.raises(RuntimeError, match="not found"):
            get_tenant_detail("missing")

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_detail_tenants_tenant_name_get")
    def test_raises_on_none(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response"):
            get_tenant_detail("kbase")


# =============================================================================
# get_tenant_members
# =============================================================================


class TestGetTenantMembers:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_members_tenants_tenant_name_members_get")
    def test_returns_members(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        member = Mock(spec=TenantMemberResponse)
        mock_api.sync.return_value = [member]

        result = get_tenant_members("kbase")

        assert result == [member]

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_members_tenants_tenant_name_members_get")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="forbidden", error_type="auth")

        with pytest.raises(RuntimeError, match="forbidden"):
            get_tenant_members("kbase")


# =============================================================================
# get_tenant_stewards
# =============================================================================


class TestGetTenantStewards:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_stewards_tenants_tenant_name_stewards_get")
    def test_returns_stewards(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        steward = Mock(spec=TenantStewardResponse)
        mock_api.sync.return_value = [steward]

        result = get_tenant_stewards("kbase")

        assert result == [steward]

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.get_tenant_stewards_tenants_tenant_name_stewards_get")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="forbidden", error_type="auth")

        with pytest.raises(RuntimeError, match="forbidden"):
            get_tenant_stewards("kbase")


# =============================================================================
# add_tenant_member
# =============================================================================


class TestAddTenantMember:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.add_tenant_member_tenants_tenant_name_members_username_post")
    def test_adds_member_read_write(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        member = Mock(spec=TenantMemberResponse, username="alice", access_level="read_write")
        mock_api.sync.return_value = member

        result = add_tenant_member("kbase", "alice")

        assert result is member
        call_kwargs = mock_api.sync.call_args
        assert call_kwargs.kwargs["tenant_name"] == "kbase"
        assert call_kwargs.kwargs["username"] == "alice"
        assert call_kwargs.kwargs["permission"].value == "read_write"

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.add_tenant_member_tenants_tenant_name_members_username_post")
    def test_adds_member_read_only(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        member = Mock(spec=TenantMemberResponse, username="bob", access_level="read_only")
        mock_api.sync.return_value = member

        result = add_tenant_member("kbase", "bob", permission="read_only")

        assert result is member
        call_kwargs = mock_api.sync.call_args
        assert call_kwargs.kwargs["permission"].value == "read_only"

    def test_invalid_permission_raises(self):
        with pytest.raises(ValueError, match="Invalid permission"):
            add_tenant_member("kbase", "alice", permission="admin")

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.add_tenant_member_tenants_tenant_name_members_username_post")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="not a member", error_type="validation")

        with pytest.raises(RuntimeError, match="not a member"):
            add_tenant_member("kbase", "alice")


# =============================================================================
# remove_tenant_member
# =============================================================================


class TestRemoveTenantMember:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.remove_tenant_member_tenants_tenant_name_members_username_delete")
    def test_removes_member(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None  # 204 No Content

        remove_tenant_member("kbase", "alice")

        mock_api.sync.assert_called_once_with(
            client=mock_client.return_value,
            tenant_name="kbase",
            username="alice",
        )

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.remove_tenant_member_tenants_tenant_name_members_username_delete")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="Cannot remove steward", error_type="forbidden")

        with pytest.raises(RuntimeError, match="Cannot remove steward"):
            remove_tenant_member("kbase", "steward_user")


# =============================================================================
# update_tenant_metadata
# =============================================================================


class TestUpdateTenantMetadata:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.update_tenant_metadata_tenants_tenant_name_patch")
    def test_updates_metadata(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        updated = Mock(spec=TenantMetadataResponse, display_name="KBase Team")
        mock_api.sync.return_value = updated

        result = update_tenant_metadata("kbase", description="Genomics team")

        assert result is updated
        call_kwargs = mock_api.sync.call_args
        assert call_kwargs.kwargs["tenant_name"] == "kbase"
        body = call_kwargs.kwargs["body"]
        assert body.description == "Genomics team"

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.update_tenant_metadata_tenants_tenant_name_patch")
    def test_updates_website(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        updated = Mock(spec=TenantMetadataResponse, website="https://www.kbase.us")
        mock_api.sync.return_value = updated

        result = update_tenant_metadata("kbase", website="https://www.kbase.us")

        assert result is updated
        body = mock_api.sync.call_args.kwargs["body"]
        assert body.website == "https://www.kbase.us"

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.update_tenant_metadata_tenants_tenant_name_patch")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="forbidden", error_type="auth")

        with pytest.raises(RuntimeError, match="forbidden"):
            update_tenant_metadata("kbase", description="test")

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.update_tenant_metadata_tenants_tenant_name_patch")
    def test_raises_on_none(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response"):
            update_tenant_metadata("kbase", description="test")


# =============================================================================
# assign_steward
# =============================================================================


class TestAssignSteward:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.assign_steward_tenants_tenant_name_stewards_username_post")
    def test_assigns_steward(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        steward = Mock(spec=TenantStewardResponse, username="alice")
        mock_api.sync.return_value = steward

        result = assign_steward("kbase", "alice")

        assert result is steward
        mock_api.sync.assert_called_once_with(
            client=mock_client.return_value,
            tenant_name="kbase",
            username="alice",
        )

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.assign_steward_tenants_tenant_name_stewards_username_post")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="already a steward", error_type="conflict")

        with pytest.raises(RuntimeError, match="already a steward"):
            assign_steward("kbase", "alice")

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.assign_steward_tenants_tenant_name_stewards_username_post")
    def test_raises_on_none(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response"):
            assign_steward("kbase", "alice")


# =============================================================================
# remove_steward
# =============================================================================


class TestRemoveSteward:
    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.remove_steward_tenants_tenant_name_stewards_username_delete")
    def test_removes_steward(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = None  # 204 No Content

        remove_steward("kbase", "alice")

        mock_api.sync.assert_called_once_with(
            client=mock_client.return_value,
            tenant_name="kbase",
            username="alice",
        )

    @patch(f"{MODULE}.get_governance_client")
    @patch(f"{MODULE}.remove_steward_tenants_tenant_name_stewards_username_delete")
    def test_raises_on_error(self, mock_api, mock_client):
        mock_client.return_value = Mock()
        mock_api.sync.return_value = ErrorResponse(message="not a steward", error_type="not_found")

        with pytest.raises(RuntimeError, match="not a steward"):
            remove_steward("kbase", "alice")


# =============================================================================
# _val helper
# =============================================================================


class TestVal:
    def test_returns_value(self):
        assert _val("hello") == "hello"

    def test_returns_none_for_none(self):
        assert _val(None) is None

    def test_returns_none_for_unset(self):
        from governance_client.types import UNSET

        assert _val(UNSET) is None


# =============================================================================
# _print_tenant
# =============================================================================


def _make_summary(name="kbase", is_member=True, is_steward=False, member_count=3):
    return Mock(
        spec=TenantSummaryResponse,
        tenant_name=name,
        is_member=is_member,
        is_steward=is_steward,
        member_count=member_count,
    )


def _make_detail(tenant_name="kbase", display_name="KBase Team", created_by="admin", created_at=None):
    from datetime import datetime, timezone

    ts = created_at or datetime(2025, 1, 15, tzinfo=timezone.utc)
    meta = Mock(
        spec=TenantMetadataResponse,
        tenant_name=tenant_name,
        display_name=display_name,
        description="A research group",
        website="https://www.kbase.us",
        organization="DOE",
        created_by=created_by,
        created_at=ts,
    )
    steward = Mock(
        spec=TenantStewardResponse,
        username="alice",
        display_name="Alice Smith",
        email="alice@org.com",
        assigned_by="admin",
    )
    member1 = Mock(
        spec=TenantMemberResponse,
        username="alice",
        display_name="Alice Smith",
        access_level=Mock(value="read_write"),
        is_steward=True,
    )
    member2 = Mock(
        spec=TenantMemberResponse,
        username="bob",
        display_name="Bob Jones",
        access_level=Mock(value="read_only"),
        is_steward=False,
    )
    paths = Mock(
        general_warehouse="s3a://cdm-lake/tenant-general-warehouse/kbase/",
        sql_warehouse="s3a://cdm-lake/tenant-sql-warehouse/kbase/",
        namespace_prefix="kbase_",
    )
    return Mock(
        spec=TenantDetailResponse,
        metadata=meta,
        stewards=[steward],
        members=[member1, member2],
        member_count=2,
        storage_paths=paths,
    )


class TestPrintTenant:
    def test_prints_tenant_info(self, capsys):
        summary = _make_summary()
        detail = _make_detail()

        _print_tenant(summary, detail, show_databases=False, all_databases=None)

        out = capsys.readouterr().out
        assert "KBase Team" in out
        assert "kbase" in out
        assert "member" in out
        assert "Alice Smith" in out
        assert "alice@org.com" in out
        assert "read_write" in out
        assert "Bob Jones" in out
        assert "read_only" in out
        assert "https://www.kbase.us" in out

    def test_steward_role_shown(self, capsys):
        summary = _make_summary(is_steward=True, is_member=True)
        detail = _make_detail()

        _print_tenant(summary, detail, show_databases=False, all_databases=None)

        out = capsys.readouterr().out
        assert "steward" in out

    def test_handles_none_created_fields(self, capsys):
        """When metadata has no created_by/created_at (no metadata row)."""
        summary = _make_summary()
        detail = _make_detail(created_by=None, created_at=None)

        _print_tenant(summary, detail, show_databases=False, all_databases=None)

        out = capsys.readouterr().out
        assert "Created by  : -" in out

    def test_databases_section(self, capsys):
        summary = _make_summary()
        detail = _make_detail()

        _print_tenant(summary, detail, show_databases=True, all_databases=["kbase_genomics", "other_db"])

        out = capsys.readouterr().out
        assert "Databases (1)" in out
        assert "kbase_genomics" in out
        assert "other_db" not in out

    def test_no_matching_databases(self, capsys):
        summary = _make_summary()
        detail = _make_detail()

        _print_tenant(summary, detail, show_databases=True, all_databases=["other_db"])

        out = capsys.readouterr().out
        assert "no databases matching" in out


# =============================================================================
# show_my_tenants
# =============================================================================


class TestShowMyTenants:
    @patch(f"{MODULE}.get_tenant_detail")
    @patch(f"{MODULE}.list_tenants")
    def test_groups_my_and_other_tenants(self, mock_list, mock_detail, capsys):
        my_tenant = _make_summary("mine", is_member=True)
        other_tenant = _make_summary("theirs", is_member=False)
        mock_list.return_value = [my_tenant, other_tenant]
        mock_detail.return_value = _make_detail()

        show_my_tenants()

        out = capsys.readouterr().out
        assert "My tenants (1)" in out
        assert "Other tenants (1)" in out
        assert mock_detail.call_count == 2

    @patch(f"{MODULE}.get_tenant_detail")
    @patch(f"{MODULE}.list_tenants")
    def test_steward_tenant_in_my_section(self, mock_list, mock_detail, capsys):
        steward_tenant = _make_summary("governed", is_member=False, is_steward=True)
        mock_list.return_value = [steward_tenant]
        mock_detail.return_value = _make_detail()

        show_my_tenants()

        out = capsys.readouterr().out
        assert "My tenants (1)" in out
        assert "Other tenants" not in out

    @patch(f"{MODULE}.list_tenants")
    def test_no_tenants(self, mock_list, capsys):
        mock_list.return_value = []

        show_my_tenants()

        out = capsys.readouterr().out
        assert "No tenants found." in out

    @patch(f"{MODULE}.get_tenant_detail")
    @patch(f"{MODULE}.list_tenants")
    def test_show_databases_flag(self, mock_list, mock_detail, capsys):
        mock_list.return_value = [_make_summary()]
        mock_detail.return_value = _make_detail()

        show_my_tenants(show_databases=False)

        out = capsys.readouterr().out
        assert "Databases" not in out
