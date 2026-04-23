from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import Row, SparkSession

from berdl_notebook_utils.berdl_settings import BERDLSettings
from berdl_notebook_utils.setup_spark_session import (
    IMMUTABLE_CONFIGS,
    SPARK_DEFAULT_POOL,
    SPARK_POOLS,
    _get_delta_conf,
    _get_executor_conf,
    _get_hive_conf,
    _get_s3_conf,
    _get_spark_defaults_conf,
    generate_spark_conf,
    get_spark_session,
)
from tests.conftest import WarehouseResponse


class FakeSparkContext:
    """
    Fake SparkContext class.

    Simplified interface checks the calls to functions when they are run and
    records the total number of calls made.
    """

    n_calls = 0
    pool = None

    def setLogLevel(self, level: str) -> None:
        assert level == "DEBUG"
        self.n_calls += 1

    def setLocalProperty(self, key: str, value: str) -> None:
        assert key == "spark.scheduler.pool"
        assert isinstance(value, str)
        self.pool = value
        self.n_calls += 1


class FakeSparkSession:
    """Fake SparkSession with simplified config and a fake sparkContext."""

    def __init__(self, conf: list[dict[str, Any]]) -> None:
        self.conf = conf
        self.context = FakeSparkContext()

    @property
    def sparkContext(self) -> FakeSparkContext:
        return self.context


class FakeBuilder:
    """Fake SparkSession builder that returns a FakeSparkSession."""

    def __init__(self) -> None:
        self.conf = {}

    def config(self, conf) -> "FakeBuilder":
        """Set the config to be used by the FakeSparkSession."""
        self.conf = conf
        return self

    def getOrCreate(self) -> "FakeSparkSession":
        """Return a FakeSparkSession initialised with the saved config."""
        return FakeSparkSession(self.conf)


@pytest.fixture(scope="module")
def monkeymodule() -> Generator[pytest.MonkeyPatch, Any, None]:
    """Set up a monkeypatch with module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


WAREHOUSE_GROUP_NAME = "group"
WAREHOUSE_USER_NAME = "user"
ALT_SPARK_CONNECT_URL = "http://www.example.com"
ALT_SPARK_MASTER_URL = "https://fake-spark.com"


@pytest.fixture(scope="module")
def alt_settings() -> BERDLSettings:
    settings = BERDLSettings()
    settings.SPARK_CONNECT_URL = ALT_SPARK_CONNECT_URL  # type: ignore
    settings.SPARK_MASTER_URL = ALT_SPARK_MASTER_URL  # type: ignore
    return settings


@pytest.fixture(scope="module")
def conf_settings(monkeymodule: pytest.MonkeyPatch) -> dict[str, Any]:
    settings = BERDLSettings()
    # patch the two functions that retrieve warehouse info to prevent external http calls
    monkeymodule.setattr(
        "berdl_notebook_utils.setup_spark_session.get_group_sql_warehouse",
        lambda _: WarehouseResponse(WAREHOUSE_GROUP_NAME),
    )
    monkeymodule.setattr(
        "berdl_notebook_utils.setup_spark_session.get_my_sql_warehouse", lambda: WarehouseResponse(WAREHOUSE_USER_NAME)
    )
    return {
        "app_name": {"spark.app.name": "my_cool_app"},
        "defaults": _get_spark_defaults_conf(),
        # executor config, legacy mode
        "exec_no_connect": _get_executor_conf(settings, False),
        # executor conf, with spark connect
        "exec_connect": _get_executor_conf(settings, True),
        # settings for delta lake
        "use_delta_lake": _get_delta_conf(),
        # hive
        "use_hive": _get_hive_conf(settings),
        # settings for using s3
        "use_s3_group": _get_s3_conf(settings, "group"),
        "use_s3_user": _get_s3_conf(settings),
    }


@pytest.mark.parametrize("app_name", [None, "my_cool_app"])
@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("use_delta_lake", [True, False])
@pytest.mark.parametrize("use_s3", [True, False])
@pytest.mark.parametrize("use_hive", [True, False])
@pytest.mark.parametrize("use_settings", [True, False])
@pytest.mark.parametrize("tenant_name", [None, "", "some_tenant"])
@pytest.mark.parametrize("use_spark_connect", [True, False])
def test_generate_spark_conf(
    monkeypatch: pytest.MonkeyPatch,
    conf_settings: dict[str, Any],
    alt_settings: BERDLSettings,
    app_name: str | None,
    local: bool,
    use_delta_lake: bool,
    use_s3: bool,
    use_hive: bool,
    use_settings: bool,
    tenant_name: str | None,
    use_spark_connect: bool,
) -> None:
    """Test the config produced by different combinations of settings for a session."""
    # set up mocks for various functions that either call external services or that require spark
    monkeypatch.setattr(
        "berdl_notebook_utils.setup_spark_session.get_group_sql_warehouse",
        lambda _: WarehouseResponse(WAREHOUSE_GROUP_NAME),
    )
    monkeypatch.setattr(
        "berdl_notebook_utils.setup_spark_session.get_my_sql_warehouse", lambda: WarehouseResponse(WAREHOUSE_USER_NAME)
    )
    settings = alt_settings if use_settings else None

    conf_dict = generate_spark_conf(
        app_name,
        local=local,
        use_delta_lake=use_delta_lake,
        use_s3=use_s3,
        use_hive=use_hive,
        settings=settings,
        tenant_name=tenant_name,
        use_spark_connect=use_spark_connect,
    )

    if not app_name:
        assert "kbase_spark_session" in conf_dict["spark.app.name"]
    else:
        assert conf_dict["spark.app.name"] == app_name

    if local:
        # delta_lake may be enabled for local sessions
        if use_delta_lake:
            assert set(conf_dict) == {"spark.app.name", *list(conf_settings["use_delta_lake"])}
        else:
            # don't expect any other keys to be set
            assert set(conf_dict) == {"spark.app.name"}
        return

    # check the defaults
    for s in conf_settings["defaults"]:
        if s in IMMUTABLE_CONFIGS and use_spark_connect:
            assert s not in conf_dict
        else:
            assert s in conf_dict

    # check executor settings
    if use_spark_connect:
        # make sure that none of the immutable vars are set
        assert all(x not in conf_dict for x in IMMUTABLE_CONFIGS)
        assert all(x in conf_dict for x in conf_settings["exec_connect"] if x not in IMMUTABLE_CONFIGS)
    else:
        assert all(x in conf_dict for x in conf_settings["exec_no_connect"])

    # check the s3 user/tenant setting is correct
    if use_s3:
        expected_s3_vals = conf_settings["use_s3_group"] if tenant_name else conf_settings["use_s3_user"]
        for k, v in expected_s3_vals.items():
            # ignore any immutable conf values if spark_connect is on
            if use_spark_connect and k in IMMUTABLE_CONFIGS:
                assert k not in conf_dict
            else:
                assert conf_dict[k] == v

        # user / tenant warehouse - only applicable if using s3 and not using spark connect
        if not use_spark_connect:
            assert conf_dict["spark.sql.warehouse.dir"] == WAREHOUSE_GROUP_NAME if tenant_name else WAREHOUSE_USER_NAME

    # hive / delta lake settings
    str_to_var = {
        "use_delta_lake": use_delta_lake,
        "use_hive": use_hive,
    }
    for var_name in str_to_var:
        for s in conf_settings[var_name]:
            # is use_hive / use_delta_lake True or False?
            if str_to_var[var_name]:
                if use_spark_connect and s in IMMUTABLE_CONFIGS:
                    assert s not in conf_dict
                else:
                    assert s in conf_dict
            else:
                assert s not in conf_dict

    # user-supplied settings
    if settings:
        if use_spark_connect:
            assert ALT_SPARK_CONNECT_URL in conf_dict["spark.remote"]
        else:
            assert conf_dict["spark.master"] == ALT_SPARK_MASTER_URL


@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize(
    ("scheduler_pool", "expected_scheduler_pool"),
    [
        (None, SPARK_DEFAULT_POOL),
        ("", SPARK_DEFAULT_POOL),
        (SPARK_POOLS[-1], SPARK_POOLS[-1]),
        ("some_pool", SPARK_DEFAULT_POOL),
    ],
)
@pytest.mark.parametrize("use_spark_connect", [True, False])
def test_get_spark_session_spark_connect(
    monkeypatch: pytest.MonkeyPatch,
    local: bool,
    scheduler_pool: str | None,
    expected_scheduler_pool: str,
    use_spark_connect: bool,
) -> None:
    """Test the config produced by different combinations of settings for a session."""
    # set up mocks for various functions that either call external services or that require spark
    monkeypatch.setattr(
        "berdl_notebook_utils.setup_spark_session.generate_spark_conf",
        lambda *args: {"this": "that"},
    )
    monkeypatch.setattr("pyspark.conf.SparkConf.setAll", lambda _, c: c)
    monkeypatch.setattr("pyspark.sql.session.SparkSession.builder", FakeBuilder())

    spark = get_spark_session(
        local=local,
        scheduler_pool=scheduler_pool,
        use_spark_connect=use_spark_connect,
    )

    assert spark is not None
    assert spark.conf == [("this", "that")]
    # check the spark connect-specific settings
    if not local and not use_spark_connect:
        # if connect is off, expect the sparkContext.logLevel to be DEBUG
        # spark.scheduler.pool should be set either with a value or with the default
        assert spark.sparkContext.n_calls == 2
        if scheduler_pool:
            assert spark.sparkContext.pool == expected_scheduler_pool
        else:
            assert spark.sparkContext.pool is SPARK_DEFAULT_POOL
    else:
        # if spark connect is on or this is a local session, none of this will have been set
        assert spark.sparkContext.n_calls == 0
        assert spark.sparkContext.pool is None


try:
    from pyspark.errors import PySparkRuntimeError
except ImportError:
    # For older pyspark versions or if pyspark is not installed (though it should be)
    class PySparkRuntimeError(Exception):
        pass


@pytest.fixture
def delta_spark(tmp_path) -> tuple[SparkSession, Path]:
    """Generate a spark session with the pytest temporary directory set as spark.sql.warehouse.dir."""
    try:
        return (
            get_spark_session(
                "test_delta_app", local=True, delta_lake=True, override={"spark.sql.warehouse.dir": tmp_path}
            ),
            tmp_path,
        )
    except (ImportError, PySparkRuntimeError) as e:
        pytest.skip(f"Spark session failed to start (likely due to Java version or missing dependencies): {e}")


@pytest.mark.requires_spark
def test_basic_local_rw(delta_spark: tuple[SparkSession, Path]) -> None:
    """Test reading and writing delta tables.

    Note that this test requires a running spark instance.
    """
    spark = delta_spark[0]
    path = delta_spark[1]
    spark_warehouse_dir = path / "some_table"

    # Create a DataFrame and save as table using returned namespace
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

    assert not spark_warehouse_dir.exists()

    # Write to temporary Delta location
    df.write.format("delta").saveAsTable("some_table")

    # check that there are files there
    assert spark_warehouse_dir.exists()
    assert spark_warehouse_dir.is_dir()

    new_df = spark.read.format("delta").load(str(spark_warehouse_dir))
    names_ids = new_df.collect()

    assert len(names_ids) == 2
    assert names_ids == [Row(id=1, name="Alice"), Row(id=2, name="Bob")]


def test_executor_conf_no_auth_token_for_legacy_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that _get_executor_conf does NOT include auth token in legacy mode."""
    from berdl_notebook_utils.berdl_settings import BERDLSettings

    settings = BERDLSettings()

    # Get config for legacy mode
    config = _get_executor_conf(settings, use_spark_connect=False)

    # Verify spark.remote URL is NOT included (legacy mode uses spark.master)
    assert "spark.remote" not in config
    # Legacy mode should have master URL instead
    assert "spark.master" in config
    assert "spark.driver.host" in config
    # Verify master URL does NOT contain auth token (legacy mode doesn't use URL-based auth)
    assert "authorization" not in config.get("spark.master", "")
