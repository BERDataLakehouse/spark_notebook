from typing import Any, Generator
from unittest.mock import Mock, patch

import pytest

from berdl_notebook_utils.spark.database import generate_namespace_location, create_namespace_if_not_exists
from tests.conftest import WarehouseResponse


USER_NAME = "someone"


class NamespacePrefix:
    """Class used for testing the response from the governance service `get_namespace_prefix`."""

    def __init__(self, **kwargs) -> None:
        self.user_namespace_prefix = "user__"
        if kwargs and "tenant" in kwargs:
            self.tenant_namespace_prefix = "tenant__"


@pytest.fixture(autouse=True)
def patch_governance(monkeypatch: pytest.MonkeyPatch) -> Generator[None, Any]:
    """Fixture to patch governance client calls used inside the functions."""
    # Patch get_my_sql_warehouse and get_group_sql_warehouse
    monkeypatch.setattr(
        "berdl_notebook_utils.spark.database.get_my_sql_warehouse",
        lambda: WarehouseResponse("s3a://cdm-lake/users-sql-warehouse/{USER_NAME}"),
    )
    monkeypatch.setattr(
        "berdl_notebook_utils.spark.database.get_group_sql_warehouse",
        lambda tenant: WarehouseResponse(f"s3a://cdm-lake/tenant-sql-warehouse/{tenant}"),
    )
    # Patch get_namespace_prefix (will be overridden in individual tests)
    monkeypatch.setattr("berdl_notebook_utils.spark.database.get_namespace_prefix", lambda **kw: NamespacePrefix(**kw))
    yield


def test_generate_namespace_location_user_warehouse() -> None:
    """Test generation of a namespace and location for a user."""
    ns, location = generate_namespace_location("analytics", tenant_name=None)
    # Expected prefix from user namespace prefix mock
    assert ns == "user__analytics"
    expected_location = "s3a://cdm-lake/users-sql-warehouse/{USER_NAME}/user__analytics.db"
    assert location == expected_location


def test_generate_namespace_location_tenant_warehouse() -> None:
    """Test generation of a namespace and location for a tenant."""
    ns, location = generate_namespace_location("reports", tenant_name="global-user-group")
    # Expected prefix from tenant namespace prefix mock
    assert ns == "tenant__reports"
    expected_location = "s3a://cdm-lake/tenant-sql-warehouse/global-user-group/tenant__reports.db"
    assert location == expected_location


def test_generate_namespace_location_no_match_warns(capfd: pytest.CaptureFixture[str]) -> None:
    """Test that a warning is emitted if the warehouse dir returned does not match expected patterns."""
    with patch(
        "berdl_notebook_utils.spark.database.get_my_sql_warehouse",
        return_value=WarehouseResponse("s3a://cdm-lake/unknown-warehouse"),
    ):
        ns, location = generate_namespace_location("default")
        # Namespace should stay unchanged, location should be None
        assert ns == "default"
        assert location is None
        captured = capfd.readouterr()
        assert "Warning: Could not determine target name from warehouse directory" in captured.out


def test_create_namespace_if_not_exists_calls_spark_sql_user() -> None:
    """Test user namespace creation (no tenant name)."""
    mock_spark = Mock()
    # Run with append_target=True (default) and no tenant => user warehouse path
    create_namespace_if_not_exists(mock_spark, namespace="data", tenant_name=None)
    # Expected namespace after prefixing
    expected_ns = "user__data"
    expected_location = "s3a://cdm-lake/users-sql-warehouse/{USER_NAME}/user__data.db"
    mock_spark.sql.assert_called_once_with(
        f"CREATE DATABASE IF NOT EXISTS {expected_ns} LOCATION '{expected_location}'"
    )


def test_create_namespace_if_not_exists_calls_spark_sql_tenant() -> None:
    """Test tenant namespace creation."""
    mock_spark = Mock()
    tenant_name = "global-users"
    create_namespace_if_not_exists(mock_spark, namespace="metrics", tenant_name=tenant_name)
    expected_ns = "tenant__metrics"
    expected_location = f"s3a://cdm-lake/tenant-sql-warehouse/{tenant_name}/tenant__metrics.db"
    mock_spark.sql.assert_called_once_with(
        f"CREATE DATABASE IF NOT EXISTS {expected_ns} LOCATION '{expected_location}'"
    )


@pytest.mark.parametrize("tenant_name", [None, "global-users"])
def test_create_namespace_if_not_exists_without_prefix(tenant_name: str | None) -> None:
    """Test namespace creation when append_target is set to false."""
    mock_spark = Mock()
    create_namespace_if_not_exists(mock_spark, namespace="raw_data", append_target=False, tenant_name=tenant_name)
    # Should create database without LOCATION clause
    mock_spark.sql.assert_called_once_with("CREATE DATABASE IF NOT EXISTS raw_data")


def test_create_namespace_if_not_exists_error() -> None:
    """Test the behaviour of create_namespace_if_not_exists if an error is thrown."""
    with (
        patch(
            "berdl_notebook_utils.spark.database.generate_namespace_location",
            side_effect=RuntimeError("things went wrong"),
        ),
        pytest.raises(RuntimeError, match="things went wrong"),
    ):
        create_namespace_if_not_exists(Mock(), "some_namespace")
