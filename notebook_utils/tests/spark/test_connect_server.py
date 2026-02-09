"""
Tests for spark/connect_server.py - Spark Connect server management.
"""

from unittest.mock import Mock, patch
import pytest

from berdl_notebook_utils.spark.connect_server import (
    SparkConnectServerConfig,
    SparkConnectServerManager,
    start_spark_connect_server,
    stop_spark_connect_server,
    get_spark_connect_status,
)


class TestSparkConnectServerConfig:
    """Tests for SparkConnectServerConfig class."""

    @patch("berdl_notebook_utils.spark.connect_server.get_settings")
    def test_init_with_default_settings(self, mock_get_settings):
        """Test config initialization with default settings."""
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_settings.SPARK_HOME = "/opt/spark"
        mock_settings.SPARK_CONNECT_DEFAULTS_TEMPLATE = "/etc/spark/template.conf"

        # Mock Pydantic URL with port
        mock_url = Mock()
        mock_url.port = 15002
        mock_settings.SPARK_CONNECT_URL = mock_url
        mock_settings.SPARK_MASTER_URL = "spark://master:7077"

        mock_get_settings.return_value = mock_settings

        config = SparkConnectServerConfig()

        assert config.username == "test_user"
        assert config.spark_connect_port == 15002
        assert "test_user" in str(config.pid_file_path)

    @patch("berdl_notebook_utils.spark.connect_server.get_settings")
    def test_init_with_custom_settings(self, mock_get_settings):
        """Test config initialization with custom settings."""
        mock_settings = Mock()
        mock_settings.USER = "custom_user"
        mock_settings.SPARK_HOME = "/custom/spark"
        mock_settings.SPARK_CONNECT_DEFAULTS_TEMPLATE = "/custom/template.conf"
        mock_url = Mock()
        mock_url.port = 15003
        mock_settings.SPARK_CONNECT_URL = mock_url
        mock_settings.SPARK_MASTER_URL = "spark://custom:7077"

        config = SparkConnectServerConfig(settings=mock_settings)

        assert config.username == "custom_user"
        assert config.spark_connect_port == 15003

    @patch("berdl_notebook_utils.spark.connect_server.get_settings")
    def test_create_directories(self, mock_get_settings, tmp_path):
        """Test create_directories creates required dirs."""
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_settings.SPARK_HOME = "/opt/spark"
        mock_settings.SPARK_CONNECT_DEFAULTS_TEMPLATE = "/etc/template.conf"
        mock_url = Mock()
        mock_url.port = 15002
        mock_settings.SPARK_CONNECT_URL = mock_url
        mock_settings.SPARK_MASTER_URL = "spark://master:7077"
        mock_get_settings.return_value = mock_settings

        config = SparkConnectServerConfig()
        config.user_conf_dir = tmp_path / "conf"
        config.connect_server_log_dir = tmp_path / "logs"

        config.create_directories()

        assert config.user_conf_dir.exists()
        assert config.connect_server_log_dir.exists()

    @patch("berdl_notebook_utils.spark.connect_server.shutil.copy")
    @patch("berdl_notebook_utils.spark.connect_server.convert_memory_format")
    @patch("berdl_notebook_utils.spark.connect_server.get_settings")
    @patch("berdl_notebook_utils.spark.connect_server.get_my_sql_warehouse")
    def test_generate_spark_config(self, mock_get_warehouse, mock_get_settings, mock_convert, mock_copy, tmp_path):
        """Test generate_spark_config creates config file."""
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_settings.SPARK_HOME = "/opt/spark"
        mock_settings.SPARK_CONNECT_DEFAULTS_TEMPLATE = str(tmp_path / "template.conf")
        mock_url = Mock()
        mock_url.port = 15002
        mock_settings.SPARK_CONNECT_URL = mock_url
        mock_settings.SPARK_MASTER_URL = "spark://master:7077"
        mock_settings.BERDL_HIVE_METASTORE_URI = "thrift://localhost:9083"
        mock_settings.MINIO_ENDPOINT_URL = "http://localhost:9000"
        mock_settings.MINIO_ACCESS_KEY = "minioadmin"
        mock_settings.MINIO_SECRET_KEY = "minioadmin"
        mock_settings.SPARK_WORKER_COUNT = 2
        mock_settings.SPARK_WORKER_CORES = 2
        mock_settings.SPARK_WORKER_MEMORY = "10G"
        mock_settings.SPARK_MASTER_CORES = 1
        mock_settings.SPARK_MASTER_MEMORY = "8G"
        mock_settings.BERDL_POD_IP = "192.168.1.100"
        mock_get_settings.return_value = mock_settings

        mock_convert.return_value = "8g"

        # Mock sql warehouse response
        mock_warehouse = Mock()
        mock_warehouse.sql_warehouse_prefix = "s3a://cdm-lake/users-sql-warehouse/test_user"
        mock_get_warehouse.return_value = mock_warehouse

        # Create template file
        template_file = tmp_path / "template.conf"
        template_file.write_text("# Base config")

        config = SparkConnectServerConfig()
        config.spark_defaults_path = tmp_path / "spark-defaults.conf"

        config.generate_spark_config()

        assert config.spark_defaults_path.exists()
        content = config.spark_defaults_path.read_text()
        assert "test_user" in content
        assert "spark.eventLog.dir" in content

    @patch("berdl_notebook_utils.spark.connect_server.get_settings")
    def test_generate_spark_config_template_not_found(self, mock_get_settings):
        """Test generate_spark_config raises if template not found."""
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_settings.SPARK_HOME = "/opt/spark"
        mock_settings.SPARK_CONNECT_DEFAULTS_TEMPLATE = "/nonexistent/template.conf"
        mock_url = Mock()
        mock_url.port = 15002
        mock_settings.SPARK_CONNECT_URL = mock_url
        mock_settings.SPARK_MASTER_URL = "spark://master:7077"
        mock_get_settings.return_value = mock_settings

        config = SparkConnectServerConfig()

        with pytest.raises(FileNotFoundError, match="Spark config template not found"):
            config.generate_spark_config()


class TestSparkConnectServerManager:
    """Tests for SparkConnectServerManager class."""

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_init_with_default_config(self, mock_config_class):
        """Test manager initialization with default config."""
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        manager = SparkConnectServerManager()

        assert manager.config == mock_config

    def test_init_with_custom_config(self):
        """Test manager initialization with custom config."""
        mock_config = Mock()

        manager = SparkConnectServerManager(config=mock_config)

        assert manager.config == mock_config

    @patch("berdl_notebook_utils.spark.connect_server.os.kill")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_get_server_info_running(self, mock_config_class, mock_kill, tmp_path):
        """Test get_server_info returns info for running server."""
        mock_config = Mock()
        mock_config.pid_file_path = tmp_path / "pid"
        mock_config.spark_connect_port = 15002
        mock_config.log_file_path = tmp_path / "log"
        mock_config.spark_master_url = "spark://master:7077"
        mock_config_class.return_value = mock_config

        # Create PID file
        (tmp_path / "pid").write_text("12345")

        manager = SparkConnectServerManager()
        info = manager.get_server_info()

        assert info["pid"] == 12345
        assert info["port"] == 15002
        assert "sc://localhost:15002" in info["url"]

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_get_server_info_not_running(self, mock_config_class, tmp_path):
        """Test get_server_info returns None when not running."""
        mock_config = Mock()
        mock_config.pid_file_path = tmp_path / "nonexistent_pid"
        mock_config_class.return_value = mock_config

        manager = SparkConnectServerManager()
        info = manager.get_server_info()

        assert info is None

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_is_running_true(self, mock_config_class, mock_get_info):
        """Test is_running returns True when server is running."""
        mock_get_info.return_value = {"pid": 12345}

        manager = SparkConnectServerManager()
        result = manager.is_running()

        assert result is True

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_is_running_false(self, mock_config_class, mock_get_info):
        """Test is_running returns False when server is not running."""
        mock_get_info.return_value = None

        manager = SparkConnectServerManager()
        result = manager.is_running()

        assert result is False

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_start_returns_existing_if_running(self, mock_config_class, mock_get_info):
        """Test start returns existing server info if already running."""
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        mock_get_info.return_value = {"pid": 12345, "port": 15002}

        manager = SparkConnectServerManager()
        result = manager.start(force_restart=False)

        assert result["pid"] == 12345

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_status_running(self, mock_config_class, mock_get_info):
        """Test status returns running status."""
        mock_config = Mock()
        mock_config.spark_connect_port = 15002
        mock_config_class.return_value = mock_config

        mock_get_info.return_value = {"pid": 12345, "port": 15002}

        manager = SparkConnectServerManager()
        result = manager.status()

        assert result["status"] == "running"
        assert result["pid"] == 12345

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_status_stopped(self, mock_config_class, mock_get_info):
        """Test status returns stopped status."""
        mock_config = Mock()
        mock_config.spark_connect_port = 15002
        mock_config_class.return_value = mock_config

        mock_get_info.return_value = None

        manager = SparkConnectServerManager()
        result = manager.status()

        assert result["status"] == "stopped"


class TestPublicApi:
    """Tests for public API functions."""

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager")
    def test_start_spark_connect_server(self, mock_manager_class):
        """Test start_spark_connect_server function."""
        mock_manager = Mock()
        mock_manager.start.return_value = {"pid": 12345}
        mock_manager_class.return_value = mock_manager

        result = start_spark_connect_server()

        assert result["pid"] == 12345
        mock_manager.start.assert_called_once_with(force_restart=False)

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager")
    def test_start_spark_connect_server_force_restart(self, mock_manager_class):
        """Test start_spark_connect_server with force_restart."""
        mock_manager = Mock()
        mock_manager.start.return_value = {"pid": 12346}
        mock_manager_class.return_value = mock_manager

        start_spark_connect_server(force_restart=True)

        mock_manager.start.assert_called_once_with(force_restart=True)

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager")
    def test_get_spark_connect_status(self, mock_manager_class):
        """Test get_spark_connect_status function."""
        mock_manager = Mock()
        mock_manager.status.return_value = {"status": "running", "pid": 12345}
        mock_manager_class.return_value = mock_manager

        result = get_spark_connect_status()

        assert result["status"] == "running"

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager")
    def test_stop_spark_connect_server(self, mock_manager_class):
        """Test stop_spark_connect_server function."""
        mock_manager = Mock()
        mock_manager.stop.return_value = True
        mock_manager_class.return_value = mock_manager

        result = stop_spark_connect_server()

        assert result is True
        mock_manager.stop.assert_called_once_with(timeout=10)

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager")
    def test_stop_spark_connect_server_custom_timeout(self, mock_manager_class):
        """Test stop_spark_connect_server with custom timeout."""
        mock_manager = Mock()
        mock_manager.stop.return_value = True
        mock_manager_class.return_value = mock_manager

        stop_spark_connect_server(timeout=30)

        mock_manager.stop.assert_called_once_with(timeout=30)


class TestSparkConnectServerManagerStop:
    """Tests for SparkConnectServerManager.stop() method."""

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager._kill_java_process")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_stop_no_server_running(self, mock_config_class, mock_get_info, mock_kill_java):
        """Test stop returns False when no server is running."""
        mock_config = Mock()
        mock_config_class.return_value = mock_config
        mock_get_info.return_value = None

        manager = SparkConnectServerManager()
        result = manager.stop()

        assert result is False
        mock_kill_java.assert_called_once()  # Still tries to kill orphaned Java process

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager._wait_for_port_release")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager._kill_java_process")
    @patch("berdl_notebook_utils.spark.connect_server.os.kill")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_stop_kills_process(self, mock_config_class, mock_get_info, mock_kill, mock_kill_java, mock_wait, tmp_path):
        """Test stop kills the process and cleans up."""
        mock_config = Mock()
        mock_config.pid_file_path = tmp_path / "pid"
        mock_config_class.return_value = mock_config

        # Create PID file
        (tmp_path / "pid").write_text("12345")

        mock_get_info.return_value = {"pid": 12345, "port": 15002}
        mock_wait.return_value = True

        manager = SparkConnectServerManager()
        result = manager.stop()

        assert result is True
        mock_kill.assert_called_once()  # Called with SIGTERM
        mock_kill_java.assert_called_once()
        mock_wait.assert_called_once()
        assert not (tmp_path / "pid").exists()  # PID file cleaned up

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager._wait_for_port_release")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager._kill_java_process")
    @patch("berdl_notebook_utils.spark.connect_server.os.kill")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.get_server_info")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_stop_handles_already_terminated(
        self, mock_config_class, mock_get_info, mock_kill, mock_kill_java, mock_wait, tmp_path
    ):
        """Test stop handles ProcessLookupError gracefully."""
        mock_config = Mock()
        mock_config.pid_file_path = tmp_path / "pid"
        mock_config_class.return_value = mock_config

        (tmp_path / "pid").write_text("12345")

        mock_get_info.return_value = {"pid": 12345, "port": 15002}
        mock_kill.side_effect = ProcessLookupError()
        mock_wait.return_value = True

        manager = SparkConnectServerManager()
        result = manager.stop()

        assert result is True  # Still succeeds

    @patch("berdl_notebook_utils.spark.connect_server.subprocess.run")
    @patch("berdl_notebook_utils.spark.connect_server.os.kill")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_kill_java_process_with_pgrep(self, mock_config_class, mock_kill, mock_run):
        """Test _kill_java_process uses pgrep to find processes."""
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        mock_run.return_value = Mock(returncode=0, stdout="12345\n12346")

        manager = SparkConnectServerManager()
        manager._kill_java_process()

        mock_run.assert_called_once()
        assert mock_kill.call_count == 2

    @patch("berdl_notebook_utils.spark.connect_server.subprocess.run")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_kill_java_process_pgrep_not_found(self, mock_config_class, mock_run):
        """Test _kill_java_process falls back to pkill when pgrep not found."""
        mock_config = Mock()
        mock_config_class.return_value = mock_config

        # First call (pgrep) raises FileNotFoundError, second call (pkill) succeeds
        mock_run.side_effect = [FileNotFoundError(), Mock()]

        manager = SparkConnectServerManager()
        manager._kill_java_process()

        assert mock_run.call_count == 2

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_wait_for_port_release_port_free(self, mock_config_class):
        """Test _wait_for_port_release returns True when port is free."""
        mock_config = Mock()
        mock_config.spark_connect_port = 59999  # Use high port unlikely to be in use
        mock_config_class.return_value = mock_config

        manager = SparkConnectServerManager()
        result = manager._wait_for_port_release(timeout=1)

        assert result is True


class TestSparkConnectServerManagerForceRestart:
    """Tests for force_restart functionality."""

    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.stop")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerManager.is_running")
    @patch("berdl_notebook_utils.spark.connect_server.SparkConnectServerConfig")
    def test_start_force_restart_calls_stop(self, mock_config_class, mock_is_running, mock_stop, tmp_path):
        """Test start with force_restart=True calls stop first."""
        mock_config = Mock()
        mock_config.username = "test_user"
        mock_config.spark_home = "/opt/spark"
        mock_config.spark_master_url = "spark://master:7077"
        mock_config.spark_connect_port = 15002
        mock_config.user_conf_dir = tmp_path / "conf"
        mock_config.log_file_path = tmp_path / "log"
        mock_config.pid_file_path = tmp_path / "pid"
        mock_config_class.return_value = mock_config

        # Server is running initially
        mock_is_running.side_effect = [True, False]  # First check: running, after stop: not running

        # Mock the start script check to fail (we don't want to actually start)
        with patch("pathlib.Path.exists", return_value=False):
            manager = SparkConnectServerManager()
            try:
                manager.start(force_restart=True)
            except FileNotFoundError:
                pass  # Expected - start script doesn't exist

        mock_stop.assert_called_once()
