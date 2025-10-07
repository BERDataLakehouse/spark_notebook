"""
Spark Connect server management for BERDL notebook environments.

This module provides utilities for starting and managing user-specific Spark Connect servers
that run alongside notebook kernels, enabling remote Spark session connectivity.
"""

import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

from ..berdl_settings import BERDLSettings, get_settings

logger = logging.getLogger(__name__)


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
        self.spark_home = os.environ.get("SPARK_HOME", "/usr/local/spark")
        self.user_spark_dir = Path.home() / ".spark"
        self.user_conf_dir = self.user_spark_dir / "conf"
        self.spark_event_log_dir = self.user_spark_dir / "spark-events"
        self.connect_server_log_dir = self.user_spark_dir / "connect-server-logs"

        # Configuration files
        self.spark_defaults_path = self.user_conf_dir / "spark-defaults.conf"
        self.template_path = os.environ.get("SPARK_DEFAULTS_TEMPLATE", "/configs/spark-defaults.conf.template")

        # Server configuration
        self.spark_connect_port = int(os.environ.get("SPARK_CONNECT_PORT", "15002"))
        self.spark_master_url = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")

        # Process management
        self.pid_file_path = Path(f"/tmp/spark-connect-server-{self.username}.pid")
        self.log_file_path = self.connect_server_log_dir / f"spark-connect-server-{self.username}.log"

    def create_directories(self) -> None:
        """Create all required directories for Spark Connect server."""
        self.user_conf_dir.mkdir(parents=True, exist_ok=True)
        self.spark_event_log_dir.mkdir(parents=True, exist_ok=True)
        self.connect_server_log_dir.mkdir(parents=True, exist_ok=True)

    def generate_spark_config(self) -> None:
        """Generate spark-defaults.conf with user-specific configurations."""
        if not Path(self.template_path).exists():
            raise FileNotFoundError(f"Spark config template not found: {self.template_path}")

        # Copy base template
        shutil.copy(self.template_path, self.spark_defaults_path)
        logger.info(f"Copied base config from {self.template_path}")

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

        logger.info(f"Spark configuration written to {self.spark_defaults_path}")


class SparkConnectServerManager:
    """Manager for Spark Connect server lifecycle."""

    def __init__(self, config: Optional[SparkConnectServerConfig] = None):
        """
        Initialize Spark Connect server manager.

        Args:
            config: SparkConnectServerConfig instance. If None, creates default config.
        """
        self.config = config or SparkConnectServerConfig()

    def is_running(self) -> bool:
        """
        Check if Spark Connect server is already running.

        Returns:
            True if server is running, False otherwise.
        """
        if not self.config.pid_file_path.exists():
            return False

        try:
            with open(self.config.pid_file_path, "r") as f:
                pid = int(f.read().strip())
            # Check if process exists (doesn't actually kill it)
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError, ValueError):
            # Process not running or invalid PID file
            self.config.pid_file_path.unlink(missing_ok=True)
            return False

    def get_server_info(self) -> Optional[dict]:
        """
        Get information about the running Spark Connect server.

        Returns:
            Dictionary with server info (pid, port, url, log_file) or None if not running.
        """
        if not self.is_running():
            return None

        with open(self.config.pid_file_path, "r") as f:
            pid = int(f.read().strip())

        return {
            "pid": pid,
            "port": self.config.spark_connect_port,
            "url": f"sc://localhost:{self.config.spark_connect_port}",
            "log_file": str(self.config.log_file_path),
            "master_url": self.config.spark_master_url,
        }

    def start(self, force_restart: bool = False) -> dict:
        """
        Start Spark Connect server.

        Args:
            force_restart: If True, stops existing server before starting new one.

        Returns:
            Dictionary with server information.
        """
        # Check if server is already running
        if self.is_running() and not force_restart:
            server_info = self.get_server_info()
            logger.info(f"✅ Spark Connect server already running (PID: {server_info['pid']})")
            logger.info("   Reusing existing server - no need to start a new one")
            return server_info

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
