from typing import Any, Generator
from unittest.mock import Mock, patch

import pytest

from berdl_notebook_utils.spark.database import (
    generate_namespace_location,
    create_namespace_if_not_exists,
    DEFAULT_NAMESPACE,
    _namespace_norm,
)
from tests.conftest import WarehouseResponse


USER_NAME = "someone"
TENANT_NAME = "group_of_someones"
USER_BASE_URL = "s3a://cdm-lake/users-sql-warehouse/"
TENANT_BASE_URL = "s3a://cdm-lake/tenant-sql-warehouse/"
NAMESPACE = "cat_photos"

EXPECTED_NS = {
    None: DEFAULT_NAMESPACE,
    "": DEFAULT_NAMESPACE,
    "\t\t\t\n": DEFAULT_NAMESPACE,
    f"   {NAMESPACE}\n\r  \r\n": NAMESPACE,
    NAMESPACE: NAMESPACE,
}


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
        lambda: WarehouseResponse(f"{USER_BASE_URL}{USER_NAME}"),
    )
    monkeypatch.setattr(
        "berdl_notebook_utils.spark.database.get_group_sql_warehouse",
        lambda tenant: WarehouseResponse(f"{TENANT_BASE_URL}{tenant}"),
    )
    # Patch get_namespace_prefix (will be overridden in individual tests)
    monkeypatch.setattr("berdl_notebook_utils.spark.database.get_namespace_prefix", lambda **kw: NamespacePrefix(**kw))
    yield


def make_mock_spark(database_exists: bool = False) -> Mock:
    """Generate a mock spark object with catalog.databaseExists mocked."""
    mock_spark = Mock(name="SparkSession")
    mock_catalog = Mock(name="Catalog")
    mock_spark.catalog = mock_catalog
    # set the return value for databaseExists
    mock_catalog.databaseExists = Mock(
        name="databaseExists",
        return_value=database_exists,
    )
    return mock_spark


def test_ns_norm_no_args() -> None:
    """Test namespace sanitization delivers the correct value if no input is supplied."""
    assert _namespace_norm() == EXPECTED_NS[None]


@pytest.mark.parametrize(("ns", "expected"), list(EXPECTED_NS.items()))
def test_ns_norm(ns: str | None, expected: str) -> None:
    """Test namespace sanitization."""
    assert _namespace_norm(ns) == expected


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_generate_namespace_location_user_tenant_warehouse(namespace_arg: str | None, tenant: str | None) -> None:
    """Test generation of a namespace and location for a user or a tenant."""
    namespace = EXPECTED_NS[namespace_arg]
    ns, location = generate_namespace_location(namespace_arg, tenant_name=tenant)  # type: ignore
    if tenant:
        assert ns == f"tenant__{namespace}"
        expected_location = f"{TENANT_BASE_URL}{TENANT_NAME}/{ns}.db"
    else:
        assert ns == f"user__{namespace}"
        expected_location = f"{USER_BASE_URL}{USER_NAME}/{ns}.db"

    assert location == expected_location


@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_generate_namespace_location_no_match_warns(
    namespace_arg: str | None, capfd: pytest.CaptureFixture[str]
) -> None:
    """Test that a warning is emitted if the warehouse dir returned does not match expected patterns."""
    with patch(
        "berdl_notebook_utils.spark.database.get_my_sql_warehouse",
        return_value=WarehouseResponse("s3a://cdm-lake/unknown-warehouse"),
    ):
        ns, location = generate_namespace_location(namespace_arg)
        # Namespace should stay unchanged, location should be None
        assert ns == EXPECTED_NS[namespace_arg]
        assert location is None
        captured = capfd.readouterr()
        assert "Warning: Could not determine target name from warehouse directory" in captured.out


# ============================================================================
# create_namespace_if_not_exists tests (Delta/Hive flow)
# ============================================================================


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_user_tenant_warehouse(namespace_arg: str | None, tenant: str | None) -> None:
    """Test user and tenant namespace creation."""
    mock_spark = make_mock_spark()
    ns = create_namespace_if_not_exists(mock_spark, namespace=namespace_arg, tenant_name=tenant)  # type: ignore
    namespace = EXPECTED_NS[namespace_arg]
    if tenant:
        assert ns == f"tenant__{namespace}"
        expected_location = f"{TENANT_BASE_URL}{TENANT_NAME}/{ns}.db"
    else:
        assert ns == f"user__{namespace}"
        expected_location = f"{USER_BASE_URL}{USER_NAME}/{ns}.db"

    mock_spark.sql.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {ns} LOCATION '{expected_location}'")


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_already_exists(
    namespace_arg: str | None, tenant: str | None, capfd: pytest.CaptureFixture[str]
) -> None:
    """Test namespace creation when the namespace has already been registered."""
    mock_spark = make_mock_spark(database_exists=True)
    ns = create_namespace_if_not_exists(mock_spark, namespace=namespace_arg, tenant_name=tenant)  # type: ignore
    namespace = EXPECTED_NS[namespace_arg]
    if tenant:
        expected_ns = f"tenant__{namespace}"
    else:
        expected_ns = f"user__{namespace}"
    assert ns == expected_ns
    # No call to spark.sql as the namespace already exists
    mock_spark.sql.assert_not_called()
    captured = capfd.readouterr()
    assert f"Namespace {expected_ns} is already registered and ready to use" in captured.out


@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_no_location_match_warns(
    namespace_arg: str | None, capfd: pytest.CaptureFixture[str]
) -> None:
    """Test that a warning is emitted if the warehouse dir returned does not match expected patterns."""
    mock_spark = make_mock_spark()
    with patch(
        "berdl_notebook_utils.spark.database.get_my_sql_warehouse",
        return_value=WarehouseResponse("s3a://cdm-lake/unknown-warehouse"),
    ):
        ns = create_namespace_if_not_exists(mock_spark, namespace_arg)
        # namespace will be unchanged
        namespace = EXPECTED_NS[namespace_arg]
        assert ns == namespace
        # Should create database without LOCATION clause
        mock_spark.sql.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {namespace}")
        captured = capfd.readouterr()
        assert "Warning: Could not determine target name from warehouse directory" in captured.out


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


# ============================================================================
# create_namespace_if_not_exists iceberg=True tests (Polaris Iceberg flow)
# ============================================================================


@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_iceberg_default_catalog(namespace_arg: str | None, capfd: pytest.CaptureFixture[str]) -> None:
    """Test Iceberg namespace creation uses 'my' catalog by default (no tenant_name)."""
    mock_spark = make_mock_spark()
    namespace = EXPECTED_NS[namespace_arg]
    result = create_namespace_if_not_exists(mock_spark, namespace, iceberg=True)
    assert result == f"my.{namespace}"
    mock_spark.sql.assert_called_once_with(f"CREATE NAMESPACE IF NOT EXISTS my.{namespace}")
    captured = capfd.readouterr()
    assert f"Namespace my.{namespace} is ready to use." in captured.out


@pytest.mark.parametrize("tenant", ["globalusers", "research"])
def test_create_namespace_iceberg_tenant_as_catalog(tenant: str) -> None:
    """Test Iceberg namespace creation uses tenant_name as catalog."""
    mock_spark = make_mock_spark()
    result = create_namespace_if_not_exists(mock_spark, "test_db", iceberg=True, tenant_name=tenant)
    assert result == f"{tenant}.test_db"
    mock_spark.sql.assert_called_once_with(f"CREATE NAMESPACE IF NOT EXISTS {tenant}.test_db")


def test_create_namespace_iceberg_no_governance_prefix() -> None:
    """Test that iceberg=True does NOT call governance API for prefixes."""
    mock_spark = make_mock_spark()
    with patch("berdl_notebook_utils.spark.database.get_namespace_prefix") as mock_prefix:
        create_namespace_if_not_exists(mock_spark, "test_db", iceberg=True)
        mock_prefix.assert_not_called()


# ============================================================================
# Cache invalidation hooks
# ============================================================================


class TestCacheInvalidationOnMutation:
    """create_namespace_if_not_exists / remove_table must wipe data_store caches."""

    def test_create_namespace_iceberg_invalidates_data_store_cache(self) -> None:
        from berdl_notebook_utils.spark import _cache as ds_cache

        ds_cache.databases_cache.set("k", ["stale"])
        ds_cache.tables_cache.set("ns", ["stale"])

        create_namespace_if_not_exists(make_mock_spark(), "new_ns", iceberg=True)

        assert ds_cache.databases_cache.get("k") is None
        assert ds_cache.tables_cache.get("ns") is None

    def test_create_namespace_delta_invalidates_data_store_cache(self) -> None:
        from berdl_notebook_utils.spark import _cache as ds_cache

        ds_cache.databases_cache.set("k", ["stale"])

        create_namespace_if_not_exists(make_mock_spark(database_exists=False), NAMESPACE)

        assert ds_cache.databases_cache.get("k") is None

    def test_create_namespace_skipped_when_exists_does_not_clear_cache(self) -> None:
        """If the namespace already existed, no mutation occurred → cache preserved."""
        from berdl_notebook_utils.spark import _cache as ds_cache

        ds_cache.databases_cache.set("k", ["fresh"])

        create_namespace_if_not_exists(make_mock_spark(database_exists=True), NAMESPACE)

        # No catalog change happened, so the cache should still be valid.
        assert ds_cache.databases_cache.get("k") == ["fresh"]

    def test_remove_table_invalidates_data_store_cache(self) -> None:
        from berdl_notebook_utils.spark import _cache as ds_cache
        from berdl_notebook_utils.spark.database import remove_table

        ds_cache.tables_cache.set("ns", ["t1", "t2"])
        ds_cache.schema_cache.set(("ns", "t1", False), ["c1"])

        remove_table(Mock(name="SparkSession"), "t1", "ns")

        assert ds_cache.tables_cache.get("ns") is None
        assert ds_cache.schema_cache.get(("ns", "t1", False)) is None
