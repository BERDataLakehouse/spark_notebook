"""
Spark Connect server management for BERDL notebook environments.

This module provides utilities for starting and managing user-specific Spark Connect servers
that run alongside notebook kernels, enabling remote Spark session connectivity.
"""

import logging
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Optional

from berdl_notebook_utils.berdl_settings import BERDLSettings, get_settings
from berdl_notebook_utils.minio_governance.operations import (
    get_my_groups,
    get_my_sql_warehouse,
    get_namespace_prefix,
)
from berdl_notebook_utils.setup_spark_session import (
    DRIVER_MEMORY_OVERHEAD,
    EXECUTOR_MEMORY_OVERHEAD,
    convert_memory_format,
)

logger = logging.getLogger(__name__)

EVENT_LOG_DIR = "s3a://cdm-spark-job-logs/spark-job-logs/"


class SparkConnectServerConfig:
    """Configuration for Spark Connect server."""

    def __init__(self, settings: Optional[BERDLSettings] = None):
        """
        Initialize Spark Connect server configuration.

        Args:
            settings: BERDLSettings instance. If None, loads from environment.
        """
        if settings is None:
            settings = get_settings()
        self.settings = settings
        self.username = settings.USER

        # Spark directories
        self.spark_home = settings.SPARK_HOME
        self.user_spark_dir = Path.home() / ".spark"
        self.user_conf_dir = self.user_spark_dir / "conf"
        self.spark_event_log_dir = EVENT_LOG_DIR + self.username
        self.connect_server_log_dir = self.user_spark_dir / "connect-server-logs"

        # Configuration files
        self.spark_defaults_path = self.user_conf_dir / "spark-defaults.conf"
        self.template_path = settings.SPARK_CONNECT_DEFAULTS_TEMPLATE

        # Server configuration - extract from BERDLSettings
        # Use validated Pydantic AnyUrl object to get port robustly
        connect_url = settings.SPARK_CONNECT_URL
        self.spark_connect_port = connect_url.port if connect_url.port is not None else 15002
        self.spark_master_url = str(settings.SPARK_MASTER_URL)

        # Process management
        self.pid_file_path = Path(f"/tmp/spark-connect-server-{self.username}.pid")
        self.log_file_path = self.connect_server_log_dir / f"spark-connect-server-{self.username}.log"

    def create_directories(self) -> None:
        """Create all required directories for Spark Connect server."""
        self.user_conf_dir.mkdir(parents=True, exist_ok=True)
        self.connect_server_log_dir.mkdir(parents=True, exist_ok=True)

    def generate_spark_config(self) -> None:
        """Generate spark-defaults.conf with user-specific configurations."""
        if not Path(self.template_path).exists():
            raise FileNotFoundError(f"Spark config template not found: {self.template_path}")

        # Copy base template
        shutil.copy(self.template_path, self.spark_defaults_path)
        logger.info(f"Copied base config from {self.template_path}")

        # Convert memory values with overhead
        executor_memory = convert_memory_format(self.settings.SPARK_WORKER_MEMORY, EXECUTOR_MEMORY_OVERHEAD)
        driver_memory = convert_memory_format(self.settings.SPARK_MASTER_MEMORY, DRIVER_MEMORY_OVERHEAD)

        # Append dynamic user-specific configurations
        with open(self.spark_defaults_path, "a") as f:
            f.write("\n# Dynamic user-specific configurations\n")
            f.write(f"# Generated for user: {self.username}\n\n")

            # Spark event log directory (for Spark History Server)
            f.write(f"spark.eventLog.dir={self.spark_event_log_dir}\n")

            # Hive metastore URI
            f.write(f"spark.hadoop.hive.metastore.uris={self.settings.BERDL_HIVE_METASTORE_URI}\n")

            # MinIO S3 configuration with user credentials
            f.write(f"spark.hadoop.fs.s3a.endpoint={self.settings.MINIO_ENDPOINT_URL}\n")
            f.write(f"spark.hadoop.fs.s3a.access.key={self.settings.MINIO_ACCESS_KEY}\n")
            f.write(f"spark.hadoop.fs.s3a.secret.key={self.settings.MINIO_SECRET_KEY}\n")

            # Spark resource configuration from profile (with overhead accounted for)
            f.write("\n# Spark cluster resource configuration\n")
            f.write(f"spark.cores.max={self.settings.SPARK_WORKER_COUNT * self.settings.SPARK_WORKER_CORES}\n")
            f.write(f"spark.executor.instances={self.settings.SPARK_WORKER_COUNT}\n")
            f.write(f"spark.executor.cores={self.settings.SPARK_WORKER_CORES}\n")
            f.write(f"spark.executor.memory={executor_memory}\n")
            f.write(f"spark.driver.cores={self.settings.SPARK_MASTER_CORES}\n")
            f.write(f"spark.driver.memory={driver_memory}\n")
            # Disable dynamic allocation since we're setting explicit instances
            f.write("spark.dynamicAllocation.enabled=false\n")
            f.write("spark.dynamicAllocation.shuffleTracking.enabled=false\n")

            f.write(f"spark.driver.host={self.settings.BERDL_POD_IP}\n")
            f.write(f"spark.master={self.settings.SPARK_MASTER_URL}\n")

            warehouse_response = get_my_sql_warehouse()
            f.write(f"spark.sql.warehouse.dir={warehouse_response.sql_warehouse_prefix}\n")

        logger.info(f"Spark configuration written to {self.spark_defaults_path}")

    def compute_allowed_namespace_prefixes(self) -> str:
        """Compute comma-separated allowed namespace prefixes for this user.

        Fetches the user's governance namespace prefix and writable tenant
        prefixes from the governance API. Read-only group memberships
        (groups ending in 'ro') are excluded since those users should not
        create databases in those tenants.

        Returns:
            Comma-separated string of allowed prefixes
            (e.g. "u_tgu2__,kbase_,research_")
        """
        prefixes = []

        # User namespace prefix (e.g. "u_tgu2__")
        try:
            ns_response = get_namespace_prefix()
            prefixes.append(ns_response.user_namespace_prefix)
        except Exception as e:
            logger.warning(f"Failed to fetch user namespace prefix: {e}")

        # Writable tenant prefixes — exclude read-only groups (ending in "ro")
        try:
            groups_response = get_my_groups()
            for group in groups_response.groups:
                if not group.endswith("ro"):
                    # Tenant prefix format: "{group}_" (matches generate_group_governance_prefix)
                    prefixes.append(f"{group}_")
        except Exception as e:
            logger.warning(f"Failed to fetch user groups: {e}")

        result = ",".join(prefixes)
        logger.info(f"Allowed namespace prefixes for {self.username}: {result}")
        return result


class SparkConnectServerManager:
    """Manager for Spark Connect server lifecycle."""

    def __init__(self, config: Optional[SparkConnectServerConfig] = None):
        """
        Initialize Spark Connect server manager.

        Args:
            config: SparkConnectServerConfig instance. If None, creates default config.
        """
        self.config = config or SparkConnectServerConfig()

    def get_server_info(self) -> Optional[dict]:
        """
        Get information about the running Spark Connect server.

        Returns:
            Dictionary with server info (pid, port, url, log_file, master_url) or None if not running.
        """
        try:
            with open(self.config.pid_file_path, "r") as f:
                pid = int(f.read().strip())

            # Verify process is still running
            os.kill(pid, 0)

            return {
                "pid": pid,
                "port": self.config.spark_connect_port,
                "url": f"sc://localhost:{self.config.spark_connect_port}",
                "log_file": str(self.config.log_file_path),
                "master_url": self.config.spark_master_url,
            }
        except (OSError, ProcessLookupError, ValueError, FileNotFoundError):
            # Process not running, invalid PID file, or PID file doesn't exist
            self.config.pid_file_path.unlink(missing_ok=True)
            return None

    def is_running(self) -> bool:
        """
        Check if Spark Connect server is already running.

        Returns:
            True if server is running, False otherwise.
        """
        return self.get_server_info() is not None

    def stop(self, timeout: int = 10) -> bool:
        """
        Stop the running Spark Connect server.

        This method kills both the shell script process (tracked by PID file)
        and the actual Java SparkConnectServer process.

        Args:
            timeout: Maximum seconds to wait for the server to stop.

        Returns:
            True if server was stopped, False if no server was running.
        """
        server_info = self.get_server_info()
        if server_info is None:
            logger.info("No Spark Connect server running (based on PID file)")
            # Still try to kill any orphaned Java process
            self._kill_java_process()
            return False

        pid = server_info["pid"]
        logger.info(f"Stopping Spark Connect server (PID: {pid})...")

        # First, try to kill the tracked shell script process
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info(f"Sent SIGTERM to process {pid}")
        except ProcessLookupError:
            logger.info(f"Process {pid} already terminated")
        except OSError as e:
            logger.warning(f"Failed to kill process {pid}: {e}")

        # Kill the actual Java SparkConnectServer process
        self._kill_java_process()

        # Clean up PID file
        self.config.pid_file_path.unlink(missing_ok=True)

        # Wait for port to be released
        self._wait_for_port_release(timeout)

        logger.info("Spark Connect server stopped")
        return True

    def _kill_java_process(self) -> None:
        """Kill the Java SparkConnectServer process."""
        try:
            # Find and kill Java SparkConnectServer process
            result = subprocess.run(
                ["pgrep", "-f", "SparkConnectServer"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                pids = result.stdout.strip().split("\n")
                for pid_str in pids:
                    if pid_str:
                        try:
                            pid = int(pid_str)
                            os.kill(pid, signal.SIGTERM)
                            logger.info(f"Killed Java SparkConnectServer process {pid}")
                        except (ValueError, ProcessLookupError, OSError) as e:
                            logger.debug(f"Could not kill process {pid_str}: {e}")
        except FileNotFoundError:
            # pgrep not available, try pkill
            try:
                subprocess.run(
                    ["pkill", "-f", "SparkConnectServer"],
                    capture_output=True,
                )
                logger.info("Sent SIGTERM to SparkConnectServer processes via pkill")
            except FileNotFoundError:
                logger.warning("Neither pgrep nor pkill available, cannot kill Java process")

    def _wait_for_port_release(self, timeout: int) -> bool:
        """
        Wait for the Spark Connect port to be released.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True if port is free, False if timeout reached.
        """
        import socket

        port = self.config.spark_connect_port
        start_time = time.time()

        while time.time() - start_time < timeout:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                result = sock.connect_ex(("localhost", port))
                if result != 0:
                    # Port is free
                    logger.info(f"Port {port} is now free")
                    return True
            finally:
                sock.close()
            time.sleep(0.5)

        logger.warning(f"Timeout waiting for port {port} to be released")
        return False

    def start(self, force_restart: bool = False) -> dict:
        """
        Start Spark Connect server.

        Args:
            force_restart: If True, stops existing server before starting new one.

        Returns:
            Dictionary with server information.
        """
        # Check if server is already running
        if self.is_running():
            if not force_restart:
                server_info = self.get_server_info()
                logger.info(f"✅ Spark Connect server already running (PID: {server_info['pid']})")
                logger.info("   Reusing existing server - no need to start a new one")
                return server_info
            else:
                # Stop the existing server before starting a new one
                logger.info("force_restart=True, stopping existing server...")
                self.stop()

        logger.info(f"Starting new Spark Connect server for user: {self.config.username}")

        # Prepare environment
        self.config.create_directories()
        self.config.generate_spark_config()

        # Verify start script exists
        start_script = Path(self.config.spark_home) / "sbin" / "start-connect-server.sh"
        if not start_script.exists():
            raise FileNotFoundError(f"Spark Connect start script not found at {start_script}")

        # Build command
        cmd = [
            str(start_script),
            "--master",
            self.config.spark_master_url,
            "--port",
            str(self.config.spark_connect_port),
        ]

        # Set environment for subprocess
        env = os.environ.copy()
        env["SPARK_NO_DAEMONIZE"] = "true"
        env["SPARK_CONF_DIR"] = str(self.config.user_conf_dir)
        env["BERDL_ALLOWED_NAMESPACE_PREFIXES"] = self.config.compute_allowed_namespace_prefixes()

        # Log startup information
        logger.info(f"Starting Spark Connect server on port {self.config.spark_connect_port}...")
        logger.info(f"Master URL: {self.config.spark_master_url}")
        logger.info(f"Config dir: {self.config.user_conf_dir}")
        logger.info(f"Log file: {self.config.log_file_path}")

        # Start the process
        with open(self.config.log_file_path, "w") as log_file:
            process = subprocess.Popen(
                cmd,
                env=env,
                cwd=str(Path.home()),  # Run from user's home directory
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # Detach from parent process
            )

        # Wait and verify startup
        time.sleep(2)

        if process.poll() is None:
            # Server started successfully
            with open(self.config.pid_file_path, "w") as f:
                f.write(str(process.pid))

            server_info = self.get_server_info()
            logger.info(f"✅ Spark Connect server started successfully (PID: {process.pid})")
            logger.info(f"   Connect URL: {server_info['url']}")
            logger.info(f"   Logs: {server_info['log_file']}")

            return server_info
        else:
            # Server failed to start
            raise RuntimeError(
                f"Spark Connect server failed to start (exit code: {process.returncode}). "
                f"Check logs: {self.config.log_file_path}"
            )

    def status(self) -> dict:
        """
        Get Spark Connect server status.

        Returns:
            Dictionary with status information.
        """
        if self.is_running():
            info = self.get_server_info()
            return {
                "status": "running",
                **info,
            }
        else:
            return {
                "status": "stopped",
                "port": self.config.spark_connect_port,
                "url": f"sc://localhost:{self.config.spark_connect_port}",
            }


# Public API - convenient functions for notebook users
def start_spark_connect_server(force_restart: bool = False) -> dict:
    """
    Start Spark Connect server for the current user.

    This function starts a Spark Connect server that runs alongside your notebook,
    allowing you to create Spark sessions using remote connections.

    Returns:
        Dictionary with server information including:
        - pid: Process ID
        - port: Server port number
        - url: Connection URL (sc://localhost:PORT)
        - log_file: Path to server logs
        - master_url: Spark master URL
    """
    manager = SparkConnectServerManager()
    return manager.start(force_restart=force_restart)


def stop_spark_connect_server(timeout: int = 10) -> bool:
    """
    Stop the running Spark Connect server.

    This function stops the Spark Connect server by killing both the shell script
    process and the Java SparkConnectServer process, then waits for the port to
    be released.

    Args:
        timeout: Maximum seconds to wait for the server to stop and port to be released.

    Returns:
        True if server was stopped, False if no server was running.
    """
    manager = SparkConnectServerManager()
    return manager.stop(timeout=timeout)


def get_spark_connect_status() -> dict:
    """
    Get Spark Connect server status.

    Returns:
        Dictionary with status information including:
        - status: "running" or "stopped"
        - pid: Process ID (if running)
        - port: Server port number
        - url: Connection URL
        - log_file: Path to server logs (if running)
        - master_url: Spark master URL (if running)
    """
    manager = SparkConnectServerManager()
    return manager.status()
