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
    add_tenant_member,
    assign_steward,
    get_my_steward_tenants,
    get_tenant_detail,
    get_tenant_members,
    get_tenant_stewards,
    list_tenants,
    remove_steward,
    remove_tenant_member,
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
