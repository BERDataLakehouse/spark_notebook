from collections.abc import Generator
from typing import Any

import pytest

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
    get_spark_session,
)


class WarehouseResponse:
    """Fake response to getting the user or group warehouse prefix."""

    def __init__(self, value: str) -> None:
        self.sql_warehouse_prefix = value


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
        "delta_lake": _get_delta_conf(),
        # hive
        "use_hive": _get_hive_conf(settings),
        # settings for using s3
        "use_s3_group": _get_s3_conf(settings, "group"),
        "use_s3_user": _get_s3_conf(settings),
    }


@pytest.mark.parametrize("app_name", [None, "my_cool_app"])
@pytest.mark.parametrize("local", [True, False])
@pytest.mark.parametrize("delta_lake", [True, False])
@pytest.mark.parametrize(
    ("scheduler_pool", "expected_scheduler_pool"),
    [
        (None, SPARK_DEFAULT_POOL),
        ("", SPARK_DEFAULT_POOL),
        (SPARK_POOLS[-1], SPARK_POOLS[-1]),
        ("some_pool", SPARK_DEFAULT_POOL),
    ],
)
@pytest.mark.parametrize("use_s3", [True, False])
@pytest.mark.parametrize("use_hive", [True, False])
@pytest.mark.parametrize("use_settings", [True, False])
@pytest.mark.parametrize("tenant_name", [None, "", "some_tenant"])
@pytest.mark.parametrize("use_spark_connect", [True, False])
def test_get_spark_session_spark_connect(
    monkeypatch: pytest.MonkeyPatch,
    conf_settings: dict[str, Any],
    alt_settings: BERDLSettings,
    app_name: str | None,
    local: bool,
    delta_lake: bool,
    scheduler_pool: str | None,
    expected_scheduler_pool: str,
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
    monkeypatch.setattr("pyspark.conf.SparkConf.setAll", lambda _, c: c)
    monkeypatch.setattr("pyspark.sql.session.SparkSession.builder", FakeBuilder())

    settings = alt_settings if use_settings else None

    spark = get_spark_session(
        app_name,
        local=local,
        delta_lake=delta_lake,
        scheduler_pool=scheduler_pool,
        use_s3=use_s3,
        use_hive=use_hive,
        settings=settings,
        tenant_name=tenant_name,
        use_spark_connect=use_spark_connect,
    )

    assert spark is not None
    conf_dict: dict[str, Any] = dict(spark.conf)
    if not app_name:
        assert "kbase_spark_session" in conf_dict["spark.app.name"]
    else:
        assert conf_dict["spark.app.name"] == app_name

    if local:
        # delta_lake may be enabled for local sessions
        if delta_lake:
            assert set(conf_dict) == {"spark.app.name", *list(conf_settings["delta_lake"])}
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
        "delta_lake": delta_lake,
        "use_hive": use_hive,
    }
    for var_name in str_to_var:
        for s in conf_settings[var_name]:
            # is use_hive / delta_lake True or False?
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
            assert conf_dict["spark.remote"] == ALT_SPARK_CONNECT_URL
        else:
            assert conf_dict["spark.master"] == ALT_SPARK_MASTER_URL

    # check the spark connect-specific settings
    if not use_spark_connect:
        # if connect is off, expect the sparkContext.logLevel to be DEBUG
        # spark.scheduler.pool should be set either with a value or with the default
        assert spark.sparkContext.n_calls == 2
        if scheduler_pool:
            assert spark.sparkContext.pool == expected_scheduler_pool
        else:
            assert spark.sparkContext.pool is SPARK_DEFAULT_POOL
    else:
        # if spark connect is on, none of this will have been set
        assert spark.sparkContext.n_calls == 0
        assert spark.sparkContext.pool is None
