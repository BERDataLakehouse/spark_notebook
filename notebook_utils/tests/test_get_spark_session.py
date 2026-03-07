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


# =============================================================================
# convert_memory_format tests
# =============================================================================


class TestConvertMemoryFormat:
    """Tests for convert_memory_format edge cases."""

    def test_invalid_memory_format_raises(self):
        """Test that invalid memory format raises ValueError."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        with pytest.raises(ValueError, match="Invalid memory format"):
            convert_memory_format("not_a_memory_value")

    def test_invalid_memory_format_no_unit(self):
        """Test that bare number without unit raises ValueError."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        with pytest.raises(ValueError, match="Invalid memory format"):
            convert_memory_format("1024")

    def test_small_memory_returns_kb(self):
        """Test small memory values return kilobyte format."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        # 1 MiB with 10% overhead = ~921.6 KiB → "922k"
        result = convert_memory_format("1MiB", 0.1)
        assert result.endswith("k")

    def test_very_small_memory_returns_bytes(self):
        """Test very small memory values (< 1 KiB) return byte format (no unit suffix)."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        # The regex requires [kmgtKMGT] prefix, so smallest valid unit is "kb"
        # 0.5 kb = 512 bytes, with 10% overhead = 460.8 bytes → "461" (no unit)
        result = convert_memory_format("0.5kb", 0.1)
        assert not result.endswith("k")
        assert not result.endswith("m")
        assert not result.endswith("g")

    def test_gib_memory(self):
        """Test GiB memory format."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        result = convert_memory_format("4GiB", 0.1)
        assert result.endswith("g")

    def test_unit_key_fallback(self):
        """Test unit key fallback for unusual unit formats."""
        from berdl_notebook_utils.setup_spark_session import convert_memory_format

        # "4gb" should work
        result = convert_memory_format("4gb", 0.1)
        assert result.endswith("g")


# =============================================================================
# _get_catalog_conf tests
# =============================================================================


class TestGetCatalogConf:
    """Tests for _get_catalog_conf with Polaris configuration."""

    def test_returns_empty_when_no_polaris_uri(self):
        """Test returns empty dict when POLARIS_CATALOG_URI is None."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = None

        result = _get_catalog_conf(settings)

        assert result == {}

    def test_personal_catalog_config(self):
        """Test generates personal catalog config when POLARIS_PERSONAL_CATALOG is set."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"  # type: ignore
        settings.POLARIS_CREDENTIAL = "client_id:client_secret"
        settings.POLARIS_PERSONAL_CATALOG = "user_tgu2"
        settings.POLARIS_TENANT_CATALOGS = None

        result = _get_catalog_conf(settings)

        assert "spark.sql.catalog.my" in result
        assert result["spark.sql.catalog.my"] == "org.apache.iceberg.spark.SparkCatalog"
        assert result["spark.sql.catalog.my.type"] == "rest"
        assert result["spark.sql.catalog.my.warehouse"] == "user_tgu2"
        assert "spark.sql.catalog.my.s3.endpoint" in result
        assert result["spark.sql.catalog.my.s3.path-style-access"] == "true"

    def test_tenant_catalog_config(self):
        """Test generates tenant catalog config with 'tenant_' prefix stripped."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"  # type: ignore
        settings.POLARIS_CREDENTIAL = "client_id:client_secret"
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = "tenant_globalusers,tenant_research"

        result = _get_catalog_conf(settings)

        # "tenant_globalusers" → alias "globalusers"
        assert "spark.sql.catalog.globalusers" in result
        assert result["spark.sql.catalog.globalusers.warehouse"] == "tenant_globalusers"
        # "tenant_research" → alias "research"
        assert "spark.sql.catalog.research" in result
        assert result["spark.sql.catalog.research.warehouse"] == "tenant_research"

    def test_empty_tenant_catalog_entries_skipped(self):
        """Test empty entries in POLARIS_TENANT_CATALOGS are skipped."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"  # type: ignore
        settings.POLARIS_CREDENTIAL = "client_id:client_secret"
        settings.POLARIS_PERSONAL_CATALOG = None
        settings.POLARIS_TENANT_CATALOGS = "tenant_kbase,,  "

        result = _get_catalog_conf(settings)

        assert "spark.sql.catalog.kbase" in result
        # Empty entries should not produce catalog configs
        catalog_keys = [
            k for k in result if k.startswith("spark.sql.catalog.") and "." not in k[len("spark.sql.catalog.") :]
        ]
        assert all("kbase" in k for k in catalog_keys)

    def test_s3_endpoint_without_http_prefix(self):
        """Test s3 endpoint gets http:// prefix if missing."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"  # type: ignore
        settings.POLARIS_CREDENTIAL = "client_id:client_secret"
        settings.POLARIS_PERSONAL_CATALOG = "user_test"
        settings.POLARIS_TENANT_CATALOGS = None
        settings.MINIO_ENDPOINT_URL = "minio:9000"

        result = _get_catalog_conf(settings)

        assert result["spark.sql.catalog.my.s3.endpoint"] == "http://minio:9000"

    def test_both_personal_and_tenant_catalogs(self):
        """Test generates config for both personal and tenant catalogs."""
        from berdl_notebook_utils.setup_spark_session import _get_catalog_conf

        settings = BERDLSettings()
        settings.POLARIS_CATALOG_URI = "http://polaris:8181/api/catalog"  # type: ignore
        settings.POLARIS_CREDENTIAL = "client_id:client_secret"
        settings.POLARIS_PERSONAL_CATALOG = "user_alice"
        settings.POLARIS_TENANT_CATALOGS = "tenant_team"

        result = _get_catalog_conf(settings)

        assert "spark.sql.catalog.my" in result
        assert "spark.sql.catalog.team" in result


# =============================================================================
# _is_immutable_config tests
# =============================================================================


class TestIsImmutableConfig:
    """Tests for _is_immutable_config function."""

    def test_known_immutable_configs(self):
        """Test known immutable config keys are detected."""
        from berdl_notebook_utils.setup_spark_session import _is_immutable_config

        for key in IMMUTABLE_CONFIGS:
            assert _is_immutable_config(key) is True, f"Expected {key} to be immutable"

    def test_catalog_config_keys_are_immutable(self):
        """Test that spark.sql.catalog.<name>.* keys are immutable."""
        from berdl_notebook_utils.setup_spark_session import _is_immutable_config

        assert _is_immutable_config("spark.sql.catalog.my") is True
        assert _is_immutable_config("spark.sql.catalog.my.type") is True
        assert _is_immutable_config("spark.sql.catalog.globalusers") is True
        assert _is_immutable_config("spark.sql.catalog.globalusers.warehouse") is True

    def test_spark_catalog_is_not_custom_catalog(self):
        """Test spark_catalog is handled by IMMUTABLE_CONFIGS set, not the prefix check."""
        from berdl_notebook_utils.setup_spark_session import _is_immutable_config

        # spark.sql.catalog.spark_catalog is in IMMUTABLE_CONFIGS explicitly
        assert _is_immutable_config("spark.sql.catalog.spark_catalog") is True

    def test_mutable_config_keys(self):
        """Test that non-immutable keys return False."""
        from berdl_notebook_utils.setup_spark_session import _is_immutable_config

        assert _is_immutable_config("spark.app.name") is False
        assert _is_immutable_config("spark.sql.autoBroadcastJoinThreshold") is False
        assert _is_immutable_config("spark.hadoop.fs.s3a.endpoint") is False
