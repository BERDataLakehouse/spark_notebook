"""
Tests for minio_governance/operations.py module.
"""

import logging
from unittest.mock import Mock, patch

import httpx
import pytest
from governance_client.models import (
    HealthResponse,
    NamespacePrefixResponse,
    PathAccessResponse,
    UserAccessiblePathsResponse,
    UserGroupsResponse,
    UserPoliciesResponse,
    UserSqlWarehousePrefixResponse,
)

from berdl_notebook_utils.minio_governance.operations import (
    _build_table_path,
    check_governance_health,
    get_minio_credentials,
    get_polaris_credentials,
    get_my_sql_warehouse,
    get_group_sql_warehouse,
    get_namespace_prefix,
    grant_namespace_access,
    get_my_workspace,
    get_my_policies,
    get_my_groups,
    get_my_accessible_paths,
    get_table_access_info,
    share_table,
    unshare_table,
    make_table_public,
    make_table_private,
    list_available_groups,
    list_groups,
    list_users,
    list_user_names,
    add_group_member,
    remove_group_member,
    create_tenant_and_assign_users,
    request_tenant_access,
    rotate_minio_credentials,
    rotate_polaris_credentials,
    list_namespace_access,
    regenerate_policies,
    revoke_namespace_access,
    CredentialsResponse,
    ErrorResponse,
    GroupManagementResponse,
    UserNamesResponse,
)


class TestBuildTablePath:
    """Tests for _build_table_path helper."""

    def test_builds_path_without_db_suffix(self):
        """Test builds path when namespace doesn't have .db suffix."""
        path = _build_table_path("user1", "analytics", "users")

        assert path == "s3a://cdm-lake/users-sql-warehouse/user1/analytics.db/users"

    def test_builds_path_with_db_suffix(self):
        """Test builds path when namespace already has .db suffix."""
        path = _build_table_path("user1", "analytics.db", "users")

        assert path == "s3a://cdm-lake/users-sql-warehouse/user1/analytics.db/users"


class TestCheckGovernanceHealth:
    """Tests for check_governance_health function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.health_check_health_get")
    def test_check_governance_health_success(self, mock_health_check, mock_get_client):
        """Test health check returns response."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_health_check.sync.return_value = Mock(spec=HealthResponse, status="healthy")

        result = check_governance_health()

        assert result.status == "healthy"

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.health_check_health_get")
    def test_check_governance_health_error_response(self, mock_health_check, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_health_check.sync.return_value = ErrorResponse(message="service down", error_type="error")

        with pytest.raises(RuntimeError, match="Health check failed: service down"):
            check_governance_health()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.health_check_health_get")
    def test_check_governance_health_none_response(self, mock_health_check, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_health_check.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            check_governance_health()


class TestGetMinioCredentials:
    """Tests for get_minio_credentials function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.os")
    @patch("berdl_notebook_utils.minio_governance.operations.get_credentials_credentials_get")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_fetches_credentials_from_api(self, mock_get_client, mock_get_creds, mock_os, mock_get_settings):
        """Test fetches credentials from API and sets env vars."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_creds = Mock(spec=CredentialsResponse)
        mock_creds.access_key = "new_key"
        mock_creds.secret_key = "new_secret"
        mock_get_creds.sync.return_value = mock_creds

        result = get_minio_credentials()

        assert result == mock_creds
        mock_get_creds.sync.assert_called_once_with(client=mock_client)
        mock_os.environ.__setitem__.assert_any_call("MINIO_ACCESS_KEY", "new_key")
        mock_os.environ.__setitem__.assert_any_call("MINIO_SECRET_KEY", "new_secret")
        mock_get_settings.cache_clear.assert_called_once()

    @patch("berdl_notebook_utils.minio_governance.operations.get_credentials_credentials_get")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_raises_on_error_response(self, mock_get_client, mock_get_creds):
        """Test raises RuntimeError when API returns an error response."""
        mock_get_client.return_value = Mock()
        mock_get_creds.sync.return_value = ErrorResponse(message="unauthorized", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to fetch credentials from API"):
            get_minio_credentials()

    @patch("berdl_notebook_utils.minio_governance.operations.get_credentials_credentials_get")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_raises_on_none_response(self, mock_get_client, mock_get_creds):
        """Test raises RuntimeError when API returns None."""
        mock_get_client.return_value = Mock()
        mock_get_creds.sync.return_value = None

        with pytest.raises(RuntimeError, match="Failed to fetch credentials from API"):
            get_minio_credentials()


class TestRotateMinioCredentials:
    """Tests for rotate_minio_credentials function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.os")
    @patch("berdl_notebook_utils.minio_governance.operations.rotate_credentials_credentials_rotate_post")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_rotates_and_updates_env_vars(self, mock_get_client, mock_rotate_api, mock_os, mock_get_settings):
        """Test rotate calls API and updates env vars."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_creds = Mock(spec=CredentialsResponse)
        mock_creds.access_key = "rotated_key"
        mock_creds.secret_key = "rotated_secret"
        mock_creds.username = "testuser"
        mock_rotate_api.sync.return_value = mock_creds

        result = rotate_minio_credentials()

        assert result == mock_creds
        mock_rotate_api.sync.assert_called_once_with(client=mock_client)
        mock_os.environ.__setitem__.assert_any_call("MINIO_ACCESS_KEY", "rotated_key")
        mock_os.environ.__setitem__.assert_any_call("MINIO_SECRET_KEY", "rotated_secret")
        mock_get_settings.cache_clear.assert_called_once()

    @patch("berdl_notebook_utils.minio_governance.operations.rotate_credentials_credentials_rotate_post")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_raises_on_error_response(self, mock_get_client, mock_rotate_api):
        """Test raises RuntimeError when API returns an error response."""
        mock_get_client.return_value = Mock()
        mock_error = Mock(spec=ErrorResponse)
        mock_rotate_api.sync.return_value = mock_error

        with pytest.raises(RuntimeError, match="Failed to rotate credentials"):
            rotate_minio_credentials()

    @patch("berdl_notebook_utils.minio_governance.operations.rotate_credentials_credentials_rotate_post")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    def test_raises_on_none_response(self, mock_get_client, mock_rotate_api):
        """Test raises RuntimeError when API returns None."""
        mock_get_client.return_value = Mock()
        mock_rotate_api.sync.return_value = None

        with pytest.raises(RuntimeError, match="Failed to rotate credentials"):
            rotate_minio_credentials()


class TestGetMySqlWarehouse:
    """Tests for get_my_sql_warehouse function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get"
    )
    def test_get_my_sql_warehouse(self, mock_get_warehouse, mock_get_client):
        """Test get_my_sql_warehouse returns warehouse prefix."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_warehouse.sync.return_value = Mock(
            spec=UserSqlWarehousePrefixResponse, sql_warehouse_prefix="s3a://bucket/prefix"
        )

        result = get_my_sql_warehouse()

        assert result.sql_warehouse_prefix == "s3a://bucket/prefix"

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get"
    )
    def test_get_my_sql_warehouse_error_response(self, mock_get_warehouse, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_warehouse.sync.return_value = ErrorResponse(message="not found", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get SQL warehouse prefix"):
            get_my_sql_warehouse()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_sql_warehouse_prefix_workspaces_me_sql_warehouse_prefix_get"
    )
    def test_get_my_sql_warehouse_none_response(self, mock_get_warehouse, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_warehouse.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_my_sql_warehouse()


class TestGetGroupSqlWarehouse:
    """Tests for get_group_sql_warehouse function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get"
    )
    def test_get_group_sql_warehouse(self, mock_get_warehouse, mock_get_client):
        """Test get_group_sql_warehouse returns group warehouse prefix."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_warehouse.sync.return_value = Mock(sql_warehouse_prefix="s3a://bucket/group")

        get_group_sql_warehouse("test_group")

        mock_get_warehouse.sync.assert_called_once_with(client=mock_client, group_name="test_group")

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get"
    )
    def test_get_group_sql_warehouse_error_response(self, mock_get_warehouse, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_warehouse.sync.return_value = ErrorResponse(message="not a member", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get group SQL warehouse prefix"):
            get_group_sql_warehouse("test_group")

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_group_sql_warehouse_prefix_workspaces_me_groups_group_name_sql_warehouse_prefix_get"
    )
    def test_get_group_sql_warehouse_none_response(self, mock_get_warehouse, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_warehouse.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_group_sql_warehouse("test_group")


class TestGetNamespacePrefix:
    """Tests for get_namespace_prefix function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_namespace_prefix_workspaces_me_namespace_prefix_get")
    def test_get_namespace_prefix_user(self, mock_get_prefix, mock_get_client):
        """Test get_namespace_prefix returns user prefix."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_prefix.sync.return_value = Mock(spec=NamespacePrefixResponse, user_namespace_prefix="u_test__")

        result = get_namespace_prefix()

        assert result.user_namespace_prefix == "u_test__"

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_namespace_prefix_workspaces_me_namespace_prefix_get")
    def test_get_namespace_prefix_tenant(self, mock_get_prefix, mock_get_client):
        """Test get_namespace_prefix returns tenant prefix when specified."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_prefix.sync.return_value = Mock(spec=NamespacePrefixResponse, tenant_namespace_prefix="t_team__")

        result = get_namespace_prefix(tenant="team")

        assert result.tenant_namespace_prefix == "t_team__"

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_namespace_prefix_workspaces_me_namespace_prefix_get")
    def test_get_namespace_prefix_error_response(self, mock_get_prefix, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_prefix.sync.return_value = ErrorResponse(message="unauthorized", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get namespace prefix"):
            get_namespace_prefix()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_namespace_prefix_workspaces_me_namespace_prefix_get")
    def test_get_namespace_prefix_none_response(self, mock_get_prefix, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_prefix.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_namespace_prefix()


class TestGetMyWorkspace:
    """Tests for get_my_workspace function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_workspace_workspaces_me_get")
    def test_get_my_workspace(self, mock_get_workspace, mock_get_client):
        """Test get_my_workspace returns workspace info."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_workspace.sync.return_value = Mock(username="test_user")

        result = get_my_workspace()

        assert result.username == "test_user"


class TestGetMyPolicies:
    """Tests for get_my_policies function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_policies_workspaces_me_policies_get")
    def test_get_my_policies(self, mock_get_policies, mock_get_client):
        """Test get_my_policies returns policy info."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_policies.sync.return_value = Mock(spec=UserPoliciesResponse, user_home_policy="policy")

        result = get_my_policies()

        assert result.user_home_policy == "policy"

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_policies_workspaces_me_policies_get")
    def test_get_my_policies_error_response(self, mock_get_policies, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_policies.sync.return_value = ErrorResponse(message="forbidden", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get policies"):
            get_my_policies()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_policies_workspaces_me_policies_get")
    def test_get_my_policies_none_response(self, mock_get_policies, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_policies.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_my_policies()


class TestGetMyGroups:
    """Tests for get_my_groups function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_groups_workspaces_me_groups_get")
    def test_get_my_groups(self, mock_get_groups, mock_get_client):
        """Test get_my_groups returns groups info."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_groups.sync.return_value = Mock(spec=UserGroupsResponse, groups=["group1", "group2"])

        result = get_my_groups()

        assert result.groups == ["group1", "group2"]

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_groups_workspaces_me_groups_get")
    def test_get_my_groups_error_response(self, mock_get_groups, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_groups.sync.return_value = ErrorResponse(message="forbidden", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get groups"):
            get_my_groups()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_groups_workspaces_me_groups_get")
    def test_get_my_groups_none_response(self, mock_get_groups, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_groups.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_my_groups()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_groups_workspaces_me_groups_get")
    def test_cache_hit_skips_api(self, mock_get_groups, mock_get_client):
        """Second call returns cached result without hitting the API again."""
        mock_get_client.return_value = Mock()
        mock_get_groups.sync.return_value = Mock(spec=UserGroupsResponse, groups=["g1"])

        result1 = get_my_groups()
        result2 = get_my_groups()

        assert result1 is result2
        mock_get_groups.sync.assert_called_once()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_my_groups_workspaces_me_groups_get")
    def test_force_refresh_bypasses_cache(self, mock_get_groups, mock_get_client):
        """force_refresh=True always hits the API."""
        mock_get_client.return_value = Mock()
        first = Mock(spec=UserGroupsResponse, groups=["g1"])
        second = Mock(spec=UserGroupsResponse, groups=["g1", "g2"])
        mock_get_groups.sync.side_effect = [first, second]

        result1 = get_my_groups()
        result2 = get_my_groups(force_refresh=True)

        assert result1.groups == ["g1"]
        assert result2.groups == ["g1", "g2"]
        assert mock_get_groups.sync.call_count == 2


class TestGetMyAccessiblePaths:
    """Tests for get_my_accessible_paths function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_accessible_paths_workspaces_me_accessible_paths_get"
    )
    def test_get_my_accessible_paths(self, mock_get_paths, mock_get_client):
        """Test get_my_accessible_paths returns accessible paths."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_paths.sync.return_value = Mock(
            spec=UserAccessiblePathsResponse, accessible_paths=["s3a://bucket/path"]
        )

        result = get_my_accessible_paths()

        assert result.accessible_paths == ["s3a://bucket/path"]

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_accessible_paths_workspaces_me_accessible_paths_get"
    )
    def test_get_my_accessible_paths_error_response(self, mock_get_paths, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_paths.sync.return_value = ErrorResponse(message="forbidden", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get accessible paths"):
            get_my_accessible_paths()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.get_my_accessible_paths_workspaces_me_accessible_paths_get"
    )
    def test_get_my_accessible_paths_none_response(self, mock_get_paths, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_get_paths.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_my_accessible_paths()


class TestGetTableAccessInfo:
    """Tests for get_table_access_info function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_path_access_info_sharing_get_path_access_info_post")
    def test_get_table_access_info(self, mock_get_access, mock_get_client, mock_settings):
        """Test get_table_access_info returns access info."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_get_access.sync.return_value = Mock(spec=PathAccessResponse, is_public=False)

        result = get_table_access_info("test_db", "test_table")

        assert result.is_public is False

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_path_access_info_sharing_get_path_access_info_post")
    def test_get_table_access_info_error_response(self, mock_get_access, mock_get_client, mock_settings):
        mock_settings.return_value.USER = "test_user"
        mock_get_client.return_value = Mock()
        mock_get_access.sync.return_value = ErrorResponse(message="not found", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to get table access info"):
            get_table_access_info("test_db", "test_table")

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_path_access_info_sharing_get_path_access_info_post")
    def test_get_table_access_info_none_response(self, mock_get_access, mock_get_client, mock_settings):
        mock_settings.return_value.USER = "test_user"
        mock_get_client.return_value = Mock()
        mock_get_access.sync.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            get_table_access_info("test_db", "test_table")


class TestShareTable:
    """Tests for share_table function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.share_data_sharing_share_post")
    def test_share_table_success(self, mock_share, mock_get_client, mock_settings):
        """Test share_table shares with users and groups."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_share.sync.return_value = Mock(success=True, errors=[])

        result = share_table("test_db", "test_table", with_users=["user1"])

        assert result.success is True

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.share_data_sharing_share_post")
    def test_share_table_logs_errors(self, mock_share, mock_get_client, mock_settings, caplog):
        """Test share_table logs errors when present."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_share.sync.return_value = Mock(errors=["User not found"])

        with caplog.at_level(logging.WARNING):
            share_table("test_db", "test_table", with_users=["invalid_user"])

        assert "Error sharing table" in caplog.text


class TestUnshareTable:
    """Tests for unshare_table function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.unshare_data_sharing_unshare_post")
    def test_unshare_table_success(self, mock_unshare, mock_get_client, mock_settings):
        """Test unshare_table removes sharing."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_unshare.sync.return_value = Mock(success=True, errors=[])

        result = unshare_table("test_db", "test_table", from_users=["user1"])

        assert result.success is True


class TestMakeTablePublic:
    """Tests for make_table_public function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.make_path_public_sharing_make_public_post")
    def test_make_table_public(self, mock_make_public, mock_get_client, mock_settings):
        """Test make_table_public makes table public."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_make_public.sync.return_value = Mock(is_public=True)

        result = make_table_public("test_db", "test_table")

        assert result.is_public is True


class TestMakeTablePrivate:
    """Tests for make_table_private function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.make_path_private_sharing_make_private_post")
    def test_make_table_private(self, mock_make_private, mock_get_client, mock_settings):
        """Test make_table_private makes table private."""
        mock_settings.return_value.USER = "test_user"
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_make_private.sync.return_value = Mock(is_public=False)

        result = make_table_private("test_db", "test_table")

        assert result.is_public is False


class TestListAvailableGroups:
    """Tests for list_available_groups function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_group_names_sync")
    def test_list_available_groups_success(self, mock_list_groups, mock_get_client):
        """Test list_available_groups returns filtered group names."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_list_groups.return_value = Mock(group_names=["kbase", "kbasero", "research", "researchro"])

        result = list_available_groups()

        assert "kbase" in result
        assert "research" in result
        assert "kbasero" not in result
        assert "researchro" not in result

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_group_names_sync")
    def test_list_available_groups_error_response(self, mock_list_groups, mock_get_client):
        """Test list_available_groups raises on error response."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_list_groups.return_value = Mock(spec=ErrorResponse, message="Error")

        with pytest.raises(RuntimeError, match="Failed to list groups"):
            list_available_groups()


class TestListGroups:
    """Tests for list_groups function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_groups_management_groups_get")
    def test_list_groups(self, mock_list_groups, mock_get_client):
        """Test list_groups returns group info."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_list_groups.sync.return_value = {"group1": ["user1", "user2"]}

        result = list_groups()

        assert result == {"group1": ["user1", "user2"]}


class TestListUsers:
    """Tests for list_users function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_users_management_users_get")
    def test_list_users(self, mock_list_users, mock_get_client):
        """Test list_users returns user info."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_list_users.sync.return_value = Mock(users=["user1", "user2"])

        result = list_users()

        assert result.users == ["user1", "user2"]


class TestAddGroupMember:
    """Tests for add_group_member function."""

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.add_group_member_management_groups_group_name_members_username_post"
    )
    def test_add_group_member_read_write(self, mock_add_member, mock_get_client, mock_time):
        """Test add_group_member adds to read/write group."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_add_member.sync.return_value = Mock(success=True)

        result = add_group_member("kbase", ["user1", "user2"])

        assert len(result) == 2
        # Verify group name doesn't have 'ro' suffix
        calls = mock_add_member.sync.call_args_list
        assert calls[0][1]["group_name"] == "kbase"

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.add_group_member_management_groups_group_name_members_username_post"
    )
    def test_add_group_member_read_only(self, mock_add_member, mock_get_client, mock_time):
        """Test add_group_member adds to read-only group."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_add_member.sync.return_value = Mock(success=True)

        add_group_member("kbase", ["user1"], read_only=True)

        # Verify group name has 'ro' suffix
        calls = mock_add_member.sync.call_args_list
        assert calls[0][1]["group_name"] == "kbasero"


class TestRemoveGroupMember:
    """Tests for remove_group_member function."""

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.remove_group_member_management_groups_group_name_members_username_delete"
    )
    def test_remove_group_member(self, mock_remove_member, mock_get_client, mock_time):
        """Test remove_group_member removes from group."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_remove_member.sync.return_value = Mock(success=True)

        result = remove_group_member("kbase", ["user1"])

        assert len(result) == 1


class TestCreateTenantAndAssignUsers:
    """Tests for create_tenant_and_assign_users function."""

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.create_group_management_groups_group_name_post")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.add_group_member_management_groups_group_name_members_username_post"
    )
    def test_create_tenant_success(self, mock_add_member, mock_create, mock_get_client, mock_time):
        """Test create_tenant_and_assign_users creates tenant and adds users."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_create.sync.return_value = Mock(success=True)
        mock_add_member.sync.return_value = Mock(success=True)

        result = create_tenant_and_assign_users("new_tenant", ["user1", "user2"])

        assert result["create_tenant"].success is True
        assert len(result["add_members"]) == 2

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.create_group_management_groups_group_name_post")
    def test_create_tenant_failure(self, mock_create, mock_get_client):
        """Test create_tenant_and_assign_users handles creation failure."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_create.sync.return_value = Mock(spec=ErrorResponse, message="Already exists")

        result = create_tenant_and_assign_users("existing_tenant")

        assert len(result["add_members"]) == 0


class TestRequestTenantAccess:
    """Tests for request_tenant_access function."""

    @patch("berdl_notebook_utils.minio_governance.operations.httpx")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_request_tenant_access_success(self, mock_settings, mock_httpx):
        """Test request_tenant_access submits request successfully."""
        mock_settings.return_value.TENANT_ACCESS_SERVICE_URL = "http://service:8000"
        mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "pending",
            "message": "Request submitted",
            "requester": "test_user",
            "tenant_name": "kbase",
            "permission": "read_only",
        }
        mock_httpx.post.return_value = mock_response

        result = request_tenant_access("kbase")

        assert result["status"] == "pending"

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_request_tenant_access_no_service_url(self, mock_settings):
        """Test request_tenant_access raises when service URL not configured."""
        mock_settings.return_value.TENANT_ACCESS_SERVICE_URL = None

        with pytest.raises(ValueError, match="TENANT_ACCESS_SERVICE_URL is not configured"):
            request_tenant_access("kbase")

    @patch("berdl_notebook_utils.minio_governance.operations.httpx")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_request_tenant_access_http_error(self, mock_settings, mock_httpx):
        """Test request_tenant_access handles HTTP errors."""
        mock_settings.return_value.TENANT_ACCESS_SERVICE_URL = "http://service:8000"
        mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

        # Make HTTPStatusError a real exception class so it can be caught
        mock_httpx.HTTPStatusError = httpx.HTTPStatusError

        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal error"
        mock_httpx.post.return_value = mock_response
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Error", request=Mock(), response=mock_response
        )

        with pytest.raises(RuntimeError, match="Failed to submit access request"):
            request_tenant_access("kbase")


# =============================================================================
# Additional tests for uncovered lines
# =============================================================================


class TestUnshareTableLogsErrors:
    """Tests for unshare_table error logging (lines 517-519)."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.unshare_data_sharing_unshare_post")
    def test_unshare_table_logs_errors(self, mock_unshare, mock_get_client, mock_settings, caplog):
        mock_settings.return_value.USER = "test_user"
        mock_get_client.return_value = Mock()
        mock_unshare.sync.return_value = Mock(errors=["User not found", "Permission denied"])

        with caplog.at_level(logging.WARNING):
            unshare_table("test_db", "test_table", from_users=["bad_user"])

        assert "Error unsharing table" in caplog.text
        assert "User not found" in caplog.text


class TestListAvailableGroupsNoneResponse:
    """Tests for list_available_groups None response (line 632)."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_group_names_sync")
    def test_list_available_groups_none_response(self, mock_list_groups, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_list_groups.return_value = None

        with pytest.raises(RuntimeError, match="Failed to list groups: no response from API"):
            list_available_groups()


class TestListUserNames:
    """Tests for list_user_names function."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_user_names_sync")
    def test_list_user_names_success(self, mock_list_names, mock_get_client):
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        mock_list_names.return_value = Mock(spec=UserNamesResponse, usernames=["alice", "bob", "charlie"])

        result = list_user_names()

        assert result == ["alice", "bob", "charlie"]
        mock_list_names.assert_called_once_with(client=mock_client)

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_user_names_sync")
    def test_list_user_names_error_response(self, mock_list_names, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_list_names.return_value = Mock(spec=ErrorResponse, message="Forbidden")

        with pytest.raises(RuntimeError, match="Failed to list usernames"):
            list_user_names()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.list_user_names_sync")
    def test_list_user_names_none_response(self, mock_list_names, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_list_names.return_value = None

        with pytest.raises(RuntimeError, match="no response from API"):
            list_user_names()


class TestCreateTenantAddMemberErrorAndException:
    """Tests for create_tenant_and_assign_users error/exception paths (lines 847, 851-856)."""

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.create_group_management_groups_group_name_post")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.add_group_member_management_groups_group_name_members_username_post"
    )
    def test_logs_warning_when_add_member_returns_error(
        self, mock_add_member, mock_create, mock_get_client, mock_time, caplog
    ):
        mock_get_client.return_value = Mock()
        mock_create.sync.return_value = Mock(spec=GroupManagementResponse)
        mock_add_member.sync.return_value = ErrorResponse(message="user not found", error_type="error")

        with caplog.at_level(logging.WARNING):
            result = create_tenant_and_assign_users("tenant1", ["baduser"])

        assert len(result["add_members"]) == 1
        assert "Failed to add user baduser" in caplog.text

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.create_group_management_groups_group_name_post")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.add_group_member_management_groups_group_name_members_username_post"
    )
    def test_handles_exception_during_add_member(
        self, mock_add_member, mock_create, mock_get_client, mock_time, caplog
    ):
        mock_get_client.return_value = Mock()
        mock_create.sync.return_value = Mock(spec=GroupManagementResponse)
        mock_add_member.sync.side_effect = [Exception("network error"), Mock(spec=GroupManagementResponse)]

        with caplog.at_level(logging.ERROR):
            result = create_tenant_and_assign_users("tenant1", ["user1", "user2"])

        # user1 failed with exception, user2 succeeded
        assert len(result["add_members"]) == 2
        username1, resp1 = result["add_members"][0]
        assert username1 == "user1"
        assert isinstance(resp1, ErrorResponse)
        assert "network error" in resp1.message
        assert "Error adding user user1" in caplog.text


class TestRequestTenantAccessJustification:
    """Tests for request_tenant_access with justification (line 930)."""

    @patch("berdl_notebook_utils.minio_governance.operations.httpx")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_includes_justification_in_payload(self, mock_settings, mock_httpx):
        mock_settings.return_value.TENANT_ACCESS_SERVICE_URL = "http://service:8000"
        mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

        mock_response = Mock()
        mock_response.json.return_value = {
            "status": "pending",
            "message": "Request submitted",
            "requester": "test_user",
            "tenant_name": "kbase",
            "permission": "read_write",
        }
        mock_httpx.post.return_value = mock_response

        result = request_tenant_access("kbase", permission="read_write", justification="Need data for project X")

        assert result["status"] == "pending"
        assert result["permission"] == "read_write"
        # Verify justification was in the payload
        call_kwargs = mock_httpx.post.call_args
        payload = call_kwargs[1]["json"]
        assert payload["justification"] == "Need data for project X"


class TestRequestTenantAccessConnectionError:
    """Tests for request_tenant_access RequestError (lines 955-956)."""

    @patch("berdl_notebook_utils.minio_governance.operations.httpx")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_request_tenant_access_connection_error(self, mock_settings, mock_httpx):
        mock_settings.return_value.TENANT_ACCESS_SERVICE_URL = "http://service:8000"
        mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

        mock_httpx.RequestError = httpx.RequestError
        mock_httpx.HTTPStatusError = httpx.HTTPStatusError
        mock_httpx.post.side_effect = httpx.RequestError("Connection refused")

        with pytest.raises(RuntimeError, match="Failed to connect to tenant access service"):
            request_tenant_access("kbase")


class TestRegeneratePolicies:
    """Tests for regenerate_policies function (lines 978-987)."""

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.regenerate_all_policies_management_migrate_regenerate_policies_post"
    )
    def test_regenerate_policies_success(self, mock_regen, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_response = Mock(users_updated=5, groups_updated=3, errors=[])
        mock_regen.sync.return_value = mock_response

        result = regenerate_policies()

        assert result.users_updated == 5
        assert result.groups_updated == 3

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.regenerate_all_policies_management_migrate_regenerate_policies_post"
    )
    def test_regenerate_policies_error_response(self, mock_regen, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_regen.sync.return_value = ErrorResponse(message="insufficient permissions", error_type="error")

        with pytest.raises(RuntimeError, match="Failed to regenerate policies: insufficient permissions"):
            regenerate_policies()

    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.regenerate_all_policies_management_migrate_regenerate_policies_post"
    )
    def test_regenerate_policies_none_response(self, mock_regen, mock_get_client):
        mock_get_client.return_value = Mock()
        mock_regen.sync.return_value = None

        with pytest.raises(RuntimeError, match="Failed to regenerate policies: no response from API"):
            regenerate_policies()


class TestRemoveGroupMemberReadOnly:
    """Tests for remove_group_member with read_only flag."""

    @patch("berdl_notebook_utils.minio_governance.operations.time")
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.remove_group_member_management_groups_group_name_members_username_delete"
    )
    def test_remove_group_member_read_only(self, mock_remove_member, mock_get_client, mock_time):
        mock_get_client.return_value = Mock()
        mock_remove_member.sync.return_value = Mock(success=True)

        result = remove_group_member("kbase", ["user1"], read_only=True)

        assert len(result) == 1
        calls = mock_remove_member.sync.call_args_list
        assert calls[0][1]["group_name"] == "kbasero"


class TestGetPolarisCredentials:
    """Tests for get_polaris_credentials function."""

    @patch("berdl_notebook_utils.minio_governance.operations.os")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_returns_none_when_polaris_not_configured(self, mock_settings, mock_os):
        """Test returns None when POLARIS_CATALOG_URI is not set."""
        mock_settings.return_value.POLARIS_CATALOG_URI = None

        result = get_polaris_credentials()

        assert result is None

    @patch("berdl_notebook_utils.minio_governance.operations.os")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.provision_polaris_user_polaris_user_provision_username_post"
    )
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_fetches_fresh_credentials(
        self,
        mock_settings,
        mock_get_client,
        mock_provision,
        mock_os,
    ):
        """Test fetches credentials from MMS and sets environment variables."""
        mock_settings.return_value.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value.USER = "test_user"

        mock_api_response = Mock()
        mock_api_response.to_dict.return_value = {
            "client_id": "new_id",
            "client_secret": "new_secret",
            "personal_catalog": "user_test_user",
            "tenant_catalogs": ["tenant_team"],
        }
        mock_provision.sync.return_value = mock_api_response

        result = get_polaris_credentials()

        assert result["client_id"] == "new_id"
        assert result["personal_catalog"] == "user_test_user"
        mock_provision.sync.assert_called_once_with(username="test_user", client=mock_get_client.return_value)
        mock_os.environ.__setitem__.assert_any_call("POLARIS_CREDENTIAL", "new_id:new_secret")
        mock_os.environ.__setitem__.assert_any_call("POLARIS_PERSONAL_CATALOG", "user_test_user")
        mock_settings.cache_clear.assert_called_once()

    @patch(
        "berdl_notebook_utils.minio_governance.operations.provision_polaris_user_polaris_user_provision_username_post"
    )
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_returns_none_on_error_response(self, mock_settings, mock_get_client, mock_provision):
        """Test returns None when API returns ErrorResponse."""
        mock_settings.return_value.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value.USER = "test_user"

        mock_error = Mock(spec=ErrorResponse)
        mock_error.message = "Internal server error"
        mock_provision.sync.return_value = mock_error

        result = get_polaris_credentials()

        assert result is None

    @patch(
        "berdl_notebook_utils.minio_governance.operations.provision_polaris_user_polaris_user_provision_username_post"
    )
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_returns_none_on_no_response(self, mock_settings, mock_get_client, mock_provision):
        """Test returns None when API returns None (unexpected status)."""
        mock_settings.return_value.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value.USER = "test_user"

        mock_provision.sync.return_value = None

        result = get_polaris_credentials()

        assert result is None


class TestRotatePolarisCredentials:
    """Tests for rotate_polaris_credentials function."""

    @patch("berdl_notebook_utils.minio_governance.operations.os")
    @patch(
        "berdl_notebook_utils.minio_governance.operations.rotate_polaris_credentials_polaris_credentials_rotate_username_post"
    )
    @patch("berdl_notebook_utils.minio_governance.operations.get_governance_client")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_rotates_current_user_credentials(
        self,
        mock_settings,
        mock_get_client,
        mock_rotate,
        mock_os,
    ):
        """Test explicit Polaris credential rotation delegates to MMS and updates env vars."""
        mock_settings.return_value.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"
        mock_settings.return_value.USER = "test_user"

        mock_api_response = Mock()
        mock_api_response.to_dict.return_value = {
            "client_id": "rotated_id",
            "client_secret": "rotated_secret",
            "personal_catalog": "user_test_user",
            "tenant_catalogs": ["tenant_team"],
        }
        mock_rotate.sync.return_value = mock_api_response

        result = rotate_polaris_credentials()

        assert result["client_id"] == "rotated_id"
        mock_rotate.sync.assert_called_once_with(username="test_user", client=mock_get_client.return_value)
        mock_os.environ.__setitem__.assert_any_call("POLARIS_CREDENTIAL", "rotated_id:rotated_secret")
        mock_settings.cache_clear.assert_called_once()

    @patch(
        "berdl_notebook_utils.minio_governance.operations.rotate_polaris_credentials_polaris_credentials_rotate_username_post",
        None,
    )
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_returns_none_when_rotate_endpoint_missing(self, mock_settings):
        """Test gracefully handles older governance clients without the rotate endpoint."""
        mock_settings.return_value.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"

        result = rotate_polaris_credentials()

        assert result is None


class TestNamespaceAccess:
    """Tests for Polaris namespace ACL notebook wrappers."""

    def _settings(self):
        settings = Mock()
        settings.GOVERNANCE_API_URL = "http://governance"
        settings.KBASE_AUTH_TOKEN = "token-123"
        settings.USER = "steward"
        return settings

    def _response(self, status_code: int = 200, payload: dict | list | None = None):
        response = Mock()
        response.status_code = status_code
        response.text = "{}" if payload is not None else ""
        response.json.return_value = payload
        return response

    @patch("builtins.print")
    @patch("berdl_notebook_utils.minio_governance.operations.httpx.request")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_grant_namespace_access_posts_payload_and_prints_refresh_hint(
        self,
        mock_settings,
        mock_request,
        mock_print,
    ):
        mock_settings.return_value = self._settings()
        payload = {
            "id": "grant-1",
            "tenant_name": "kbase",
            "namespace": ["shared_data"],
            "namespace_name": "shared_data",
            "username": "alice",
            "access_level": "write",
            "status": "active",
        }
        mock_request.return_value = self._response(201, payload)

        result = grant_namespace_access("kbase", "alice", "shared_data", access_level="write")

        assert result == payload
        mock_request.assert_called_once_with(
            "POST",
            "http://governance/tenants/kbase/namespace-acls",
            headers={"Authorization": "Bearer token-123", "Content-Type": "application/json"},
            json={"username": "alice", "namespace": ["shared_data"], "access_level": "write"},
            params=None,
            timeout=30.0,
        )
        assert "refresh_spark_environment()" in mock_print.call_args.args[0]
        assert "kbase.shared_data" in mock_print.call_args.args[0]

    @patch("builtins.print")
    @patch("berdl_notebook_utils.minio_governance.operations.httpx.request")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_revoke_namespace_access_sends_delete(self, mock_settings, mock_request, mock_print):
        mock_settings.return_value = self._settings()
        payload = {"id": "grant-1", "status": "revoked"}
        mock_request.return_value = self._response(200, payload)

        result = revoke_namespace_access("kbase", "alice", ["geo", "curated"])

        assert result == payload
        mock_request.assert_called_once_with(
            "DELETE",
            "http://governance/tenants/kbase/namespace-acls",
            headers={"Authorization": "Bearer token-123", "Content-Type": "application/json"},
            json={"username": "alice", "namespace": ["geo", "curated"]},
            params=None,
            timeout=30.0,
        )
        assert "refresh_spark_environment()" in mock_print.call_args.args[0]
        assert "kbase.geo.curated" in mock_print.call_args.args[0]

    @patch("berdl_notebook_utils.minio_governance.operations.httpx.request")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_list_namespace_access_for_current_user(self, mock_settings, mock_request):
        mock_settings.return_value = self._settings()
        payload = [{"id": "grant-1"}]
        mock_request.return_value = self._response(200, payload)

        result = list_namespace_access()

        assert result == payload
        mock_request.assert_called_once_with(
            "GET",
            "http://governance/me/namespace-acls",
            headers={"Authorization": "Bearer token-123", "Content-Type": "application/json"},
            json=None,
            params=None,
            timeout=30.0,
        )

    @patch("berdl_notebook_utils.minio_governance.operations.httpx.request")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_list_namespace_access_for_tenant_with_namespace_filter(self, mock_settings, mock_request):
        mock_settings.return_value = self._settings()
        payload = [{"id": "grant-1"}]
        mock_request.return_value = self._response(200, payload)

        result = list_namespace_access("kbase", namespace=["geo", "curated"])

        assert result == payload
        mock_request.assert_called_once_with(
            "GET",
            "http://governance/tenants/kbase/namespace-acls",
            headers={"Authorization": "Bearer token-123", "Content-Type": "application/json"},
            json=None,
            params={"namespace": "geo.curated"},
            timeout=30.0,
        )

    def test_grant_namespace_access_rejects_invalid_access_level(self):
        with pytest.raises(ValueError, match="access_level"):
            grant_namespace_access("kbase", "alice", "shared_data", access_level="admin")

    def test_list_namespace_access_rejects_filter_without_tenant(self):
        with pytest.raises(ValueError, match="requires tenant_name"):
            list_namespace_access(namespace="shared_data")

    @patch("berdl_notebook_utils.minio_governance.operations.httpx.request")
    @patch("berdl_notebook_utils.minio_governance.operations.get_settings")
    def test_namespace_access_raises_runtime_error_on_api_error(self, mock_settings, mock_request):
        mock_settings.return_value = self._settings()
        mock_request.return_value = self._response(409, {"detail": "Namespace has child namespaces"})

        with pytest.raises(RuntimeError, match="Namespace has child namespaces"):
            grant_namespace_access("kbase", "alice", "shared_data", show_refresh_hint=False)
