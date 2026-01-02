import logging
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import Row

from berdl_notebook_utils.spark.database import (
    DEFAULT_NAMESPACE,
    _namespace_norm,
    create_namespace_if_not_exists,
    generate_namespace_location,
    get_namespace_info,
    get_table_info,
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
    monkeypatch.setattr("berdl_notebook_utils.spark.database.get_namespace_prefix", NamespacePrefix)
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


def make_mock_spark_sql_error() -> Mock:
    """Generate a mock spark object that objects to being asked to perform SQL operations."""
    mock_spark = Mock(name="SparkSession")
    # no SQLs here
    mock_spark.sql = Mock(side_effect=RuntimeError("Things go wrong: the SQL"))
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
    namespace_arg: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a warning is emitted if the warehouse dir returned does not match expected patterns."""
    caplog.set_level(logging.INFO)
    with patch(
        "berdl_notebook_utils.spark.database.get_my_sql_warehouse",
        return_value=WarehouseResponse("s3a://cdm-lake/unknown-warehouse"),
    ):
        ns, location = generate_namespace_location(namespace_arg)
        # Namespace should stay unchanged, location should be None
        assert ns == EXPECTED_NS[namespace_arg]
        assert location is None
        logs = caplog.records
        assert logs[0].levelno == logging.WARNING
        assert logs[0].message.startswith("Warning: Could not determine target name from warehouse directory")


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_user_tenant_warehouse(
    namespace_arg: str | None, tenant: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test user and tenant namespace creation."""
    caplog.set_level(logging.INFO)
    mock_spark = make_mock_spark()
    # Run with append_target=True (default)
    ns = create_namespace_if_not_exists(mock_spark, namespace=namespace_arg, tenant_name=tenant)  # type: ignore
    namespace = EXPECTED_NS[namespace_arg]
    if tenant:
        namespace = f"tenant__{namespace}"
        expected_location = f"{TENANT_BASE_URL}{TENANT_NAME}/{ns}.db"
    else:
        namespace = f"user__{namespace}"
        expected_location = f"{USER_BASE_URL}{USER_NAME}/{ns}.db"
    assert ns == namespace
    mock_spark.sql.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {ns} LOCATION '{expected_location}'")
    logs = caplog.records
    assert logs[0].levelno == logging.INFO
    assert logs[0].message.startswith(f"Namespace {namespace} is ready to use at location {expected_location}")


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_without_prefix(
    namespace_arg: str | None, tenant: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test namespace creation when append_target is set to false."""
    caplog.set_level(logging.INFO)
    mock_spark = make_mock_spark()
    ns = create_namespace_if_not_exists(mock_spark, namespace=namespace_arg, append_target=False, tenant_name=tenant)  # type: ignore
    namespace = EXPECTED_NS[namespace_arg]
    assert ns == namespace
    # Should create database without LOCATION clause
    mock_spark.sql.assert_called_once_with(f"CREATE DATABASE IF NOT EXISTS {namespace}")

    logs = caplog.records
    assert logs[0].levelno == logging.INFO
    assert logs[0].message == f"Namespace {namespace} is ready to use."


@pytest.mark.parametrize("tenant", [None, TENANT_NAME])
@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_already_exists(
    namespace_arg: str | None, tenant: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test namespace creation when the namespace has already been registered."""
    caplog.set_level(logging.INFO)
    mock_spark = make_mock_spark(database_exists=True)
    ns = create_namespace_if_not_exists(mock_spark, namespace=namespace_arg, append_target=False, tenant_name=tenant)  # type: ignore
    namespace = EXPECTED_NS[namespace_arg]
    assert ns == namespace
    # No call to spark.sql as the namespace already exists
    mock_spark.sql.assert_not_called()
    logs = caplog.records
    assert logs[0].levelno == logging.INFO
    assert logs[0].message == f"Namespace {namespace} is already registered and ready to use."


@pytest.mark.parametrize("namespace_arg", EXPECTED_NS)
def test_create_namespace_if_not_exists_no_location_match_warns(
    namespace_arg: str | None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that a warning is emitted if the warehouse dir returned does not match expected patterns."""
    caplog.set_level(logging.INFO)
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

        # logs will contain the warning about the target name and the namespace created message
        logs = caplog.records
        assert logs[0].levelno == logging.WARNING
        assert logs[0].message.startswith("Warning: Could not determine target name from warehouse directory")


def test_create_namespace_if_not_exists_error(caplog: pytest.LogCaptureFixture) -> None:
    """Test the behaviour of create_namespace_if_not_exists if an error is thrown."""
    caplog.set_level(logging.INFO)
    with (
        patch(
            "berdl_notebook_utils.spark.database.generate_namespace_location",
            side_effect=RuntimeError("things went wrong"),
        ),
        pytest.raises(RuntimeError, match="things went wrong"),
    ):
        create_namespace_if_not_exists(Mock(), "some_namespace")
    logs = caplog.records
    assert logs[0].levelno == logging.ERROR
    assert logs[0].message.startswith("Error creating namespace")


def test_get_table_info() -> None:
    """Test the retrieval and reformatting of table information."""
    table_data = [
        # only one column in this table!
        Row(col_name="a", data_type="string", comment=None),
        Row(col_name="", data_type="", comment=""),
        Row(col_name="# Detailed Table Information", data_type="", comment=""),
        Row(col_name="Name", data_type="spark_catalog.who_cares.whatever", comment=""),
        Row(col_name="Type", data_type="MANAGED", comment=""),
        Row(
            col_name="Location",
            data_type=f"{TENANT_BASE_URL}/some_tenant/default.db/table",
            comment="",
        ),
        Row(col_name="Provider", data_type="delta", comment=""),
        Row(col_name="Table Properties", data_type="[delta.minReaderVersion=1,delta.minWriterVersion=2]", comment=""),
    ]
    mock_df = MagicMock()
    mock_df.collect.return_value = table_data

    mock_spark = MagicMock()
    mock_spark.sql.return_value = mock_df

    output = get_table_info(mock_spark, "whatever", "who_cares")
    assert output == {
        "a": "string",
        "Name": "spark_catalog.who_cares.whatever",
        "Type": "MANAGED",
        "Location": f"{TENANT_BASE_URL}/some_tenant/default.db/table",
        "Provider": "delta",
        "Table Properties": "[delta.minReaderVersion=1,delta.minWriterVersion=2]",
    }


def test_get_table_info_error(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that an error message is logged if something happens when retrieving table info."""
    caplog.set_level(logging.INFO)
    mock_spark = make_mock_spark_sql_error()
    output = get_table_info(
        mock_spark,
        "some_table",
        "some_namespace",
    )
    assert output == {}
    logs = caplog.records
    assert logs[0].levelno == logging.ERROR
    assert logs[0].message.startswith("Error getting table info for some_namespace.some_table")


def test_get_namespace_info() -> None:
    """Test the retrieval and reformatting of namespace information."""
    ns_data = [
        Row(info_name="Catalog Name", info_value="spark_catalog"),
        Row(info_name="Namespace Name", info_value="SuperCoolDataOnly"),
        Row(info_name="Comment", info_value=""),
        Row(info_name="Location", info_value="/path/to/wherever"),
        Row(info_name="Owner", info_value="root"),
        Row(info_name="Properties", info_value=""),
    ]
    mock_df = MagicMock()
    mock_df.collect.return_value = ns_data

    mock_spark = MagicMock()
    mock_spark.sql.return_value = mock_df

    output = get_namespace_info(mock_spark, "whatever")
    assert output == {
        "Catalog Name": "spark_catalog",
        "Namespace Name": "SuperCoolDataOnly",
        "Comment": "",
        "Location": "/path/to/wherever",
        "Owner": "root",
        "Properties": "",
    }


def test_get_namespace_info_error(caplog: pytest.LogCaptureFixture) -> None:
    """Ensure that an error message is logged if something happens when retrieving namespace info."""
    caplog.set_level(logging.INFO)
    mock_spark = make_mock_spark_sql_error()
    output = get_namespace_info(mock_spark, "some_namespace")
    assert output == {}
    logs = caplog.records
    assert logs[0].levelno == logging.ERROR
    assert logs[0].message.startswith("Error getting namespace info for some_namespace")
