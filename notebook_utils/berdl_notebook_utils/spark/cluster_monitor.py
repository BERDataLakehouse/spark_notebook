"""
Spark Cluster Monitoring Utilities

This module provides comprehensive monitoring and inspection capabilities for Spark clusters,
including worker node information, resource usage, application status, and performance metrics.

Example Usage:
    # Basic cluster monitoring
    from berdl_notebook_utils.spark.cluster_monitor import SparkClusterMonitor

    # Initialize monitor (uses active Spark session)
    monitor = SparkClusterMonitor()

    # Get cluster summary
    summary = monitor.get_cluster_summary()
    print(f"Total cores: {summary.cores_total}")
    print(f"Used cores: {summary.cores_used}")
    print(f"Memory usage: {summary.memory_used_gb:.1f}GB/{summary.memory_total_gb:.1f}GB")

    # Display formatted cluster status
    monitor.display_cluster_summary()
    monitor.display_worker_details()
    monitor.display_application_details()

    # Get worker node details
    workers = monitor.get_worker_nodes()
    for worker in workers:
        print(f"Worker {worker.worker_id}: {worker.cores_used}/{worker.cores} cores")

    # Get application information
    apps = monitor.get_applications()
    for app in apps:
        print(f"App {app.app_name}: {app.state} - {app.cores_granted} cores")

    # Quick access functions
    from berdl_notebook_utils.spark.cluster_monitor import (
        get_cluster_info, show_cluster_status, get_resource_usage
    )

    # Get cluster summary quickly
    cluster_info = get_cluster_info()

    # Show complete cluster status
    show_cluster_status()

    # Get resource utilization percentages
    usage = get_resource_usage()
    print(f"Core utilization: {usage['core_utilization_percent']:.1f}%")
    print(f"Memory utilization: {usage['memory_utilization_percent']:.1f}%")
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import SparkSession


@dataclass
class WorkerNode:
    """Information about a Spark worker node."""

    worker_id: str
    host: str
    port: int
    cores: int
    cores_used: int
    cores_free: int
    memory_mb: int
    memory_used_mb: int
    memory_free_mb: int
    active_drivers: int
    executors: int
    state: str
    last_heartbeat: str


@dataclass
class SparkApplication:
    """Information about a Spark application."""

    app_id: str
    app_name: str
    user: str
    state: str
    duration: int
    start_time: str
    end_time: Optional[str]
    cores_granted: int
    max_cores: int
    memory_per_executor_mb: int
    executors: int


@dataclass
class ExecutorInfo:
    """Information about a Spark executor."""

    executor_id: str
    host: str
    port: int
    cores: int
    memory_mb: int
    state: str
    app_id: str
    app_name: str


@dataclass
class ClusterSummary:
    """Summary of the entire Spark cluster."""

    cluster_url: str
    status: str
    workers: int
    cores_total: int
    cores_used: int
    cores_free: int
    memory_total_gb: float
    memory_used_gb: float
    memory_free_gb: float
    applications_running: int
    applications_completed: int
    alive_workers: int
    dead_workers: int


class SparkClusterMonitor:
    """
    Comprehensive Spark cluster monitoring utility.

    Provides methods to inspect cluster state, worker nodes, applications,
    and resource usage through both Spark API and web UI endpoints.
    """

    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the cluster monitor.

        Args:
            spark_session: Active Spark session. If None, gets the active session.
        """
        self.spark = spark_session or SparkSession.getActiveSession()
        if not self.spark:
            raise ValueError("No active Spark session found. Please create a Spark session first.")

        self.sc = self.spark.sparkContext
        self.master_url = self.sc.master

        # Extract web UI URL from master URL
        if self.master_url and "spark://" in self.master_url:
            # Convert spark://host:port to http://host:webui_port
            host = self.master_url.replace("spark://", "").split(":")[0]
            self.web_ui_url = f"http://{host}:8080"  # Default Spark master web UI port
        else:
            self.web_ui_url = None

    def _fetch_json_from_url(self, url: str) -> Optional[Dict]:
        """Fetch JSON data from a URL with error handling."""
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not fetch data from {url}: {e}")
            return None

    def get_cluster_summary(self) -> ClusterSummary:
        """Get a comprehensive summary of the cluster state."""
        if not self.web_ui_url:
            return self._get_basic_cluster_summary()

        # Fetch data from Spark master web UI API
        workers_data = self._fetch_json_from_url(f"{self.web_ui_url}/json")

        if not workers_data:
            return self._get_basic_cluster_summary()

        # Parse worker information
        workers = workers_data.get("workers", [])
        alive_workers = [w for w in workers if w["state"] == "ALIVE"]
        dead_workers = [w for w in workers if w["state"] != "ALIVE"]

        cores_total = sum(w["cores"] for w in alive_workers)
        cores_used = sum(w["coresused"] for w in alive_workers)
        memory_total_mb = sum(w["memory"] for w in alive_workers)
        memory_used_mb = sum(w["memoryused"] for w in alive_workers)

        # Parse application information from workers_data (same source as get_applications)
        running_apps = len(workers_data.get("activeapps", []))
        completed_apps = len(workers_data.get("completedapps", []))

        return ClusterSummary(
            cluster_url=self.master_url or "unknown",
            status=workers_data.get("status", "UNKNOWN"),
            workers=len(workers),
            cores_total=cores_total,
            cores_used=cores_used,
            cores_free=cores_total - cores_used,
            memory_total_gb=memory_total_mb / 1024,
            memory_used_gb=memory_used_mb / 1024,
            memory_free_gb=(memory_total_mb - memory_used_mb) / 1024,
            applications_running=running_apps,
            applications_completed=completed_apps,
            alive_workers=len(alive_workers),
            dead_workers=len(dead_workers),
        )

    def _get_basic_cluster_summary(self) -> ClusterSummary:
        """Get basic cluster summary when web UI is not available."""
        raise RuntimeError(
            "Cluster monitoring requires access to Spark Web UI. "
            "Web UI is not available (likely running in local mode or Web UI is disabled). "
            "Please ensure Spark cluster is running with Web UI enabled."
        )

    def get_worker_nodes(self) -> List[WorkerNode]:
        """Get detailed information about all worker nodes."""
        if not self.web_ui_url:
            return self._get_basic_worker_info()

        data = self._fetch_json_from_url(f"{self.web_ui_url}/json")
        if not data:
            return self._get_basic_worker_info()

        # Get cluster-level information for worker context
        cluster_active_drivers = len(data.get("activedrivers", []))

        workers = []
        for worker_data in data.get("workers", []):
            # Convert timestamp to readable format if it's a number
            last_heartbeat = worker_data.get("lastheartbeat", "unknown")
            if isinstance(last_heartbeat, (int, float)):
                from datetime import datetime

                last_heartbeat = datetime.fromtimestamp(last_heartbeat / 1000).isoformat()

            workers.append(
                WorkerNode(
                    worker_id=worker_data.get("id", "unknown"),
                    host=worker_data.get("host", "unknown"),
                    port=worker_data.get("port", 0),
                    cores=worker_data.get("cores", 0),
                    cores_used=worker_data.get("coresused", 0),
                    cores_free=worker_data.get("coresfree", 0),
                    memory_mb=worker_data.get("memory", 0),
                    memory_used_mb=worker_data.get("memoryused", 0),
                    memory_free_mb=worker_data.get("memoryfree", 0),
                    # These fields don't exist in worker data - use cluster-level data or defaults
                    active_drivers=cluster_active_drivers,  # Total cluster drivers
                    executors=0,  # Will be calculated from applications if needed
                    state=worker_data.get("state", "UNKNOWN"),
                    last_heartbeat=str(last_heartbeat),
                )
            )

        return workers

    def _get_basic_worker_info(self) -> List[WorkerNode]:
        """Get basic worker info when web UI is not available."""
        raise RuntimeError(
            "Worker node monitoring requires access to Spark Web UI. "
            "Web UI is not available (likely running in local mode or Web UI is disabled). "
            "Please ensure Spark cluster is running with Web UI enabled."
        )

    def get_applications(self) -> List[SparkApplication]:
        """Get information about all Spark applications."""
        if not self.web_ui_url:
            return self._get_current_application_info()

        # Get cluster data which contains application information
        data = self._fetch_json_from_url(f"{self.web_ui_url}/json")
        if not data:
            return self._get_current_application_info()

        applications = []

        # Process active applications
        for app_data in data.get("activeapps", []):
            # Convert timestamp if it's a number
            start_time = app_data.get("starttime", 0)
            if isinstance(start_time, (int, float)):
                from datetime import datetime

                start_time = datetime.fromtimestamp(start_time / 1000).isoformat()

            applications.append(
                SparkApplication(
                    app_id=app_data.get("id", "unknown"),
                    app_name=app_data.get("name", "unknown"),
                    user=app_data.get("user", "unknown"),
                    state=app_data.get("state", "UNKNOWN"),
                    duration=app_data.get("duration", 0),
                    start_time=str(start_time),
                    end_time=None,  # Active apps don't have end time
                    cores_granted=app_data.get("cores", 0),
                    max_cores=app_data.get("cores", 0),  # Use same as granted for active apps
                    memory_per_executor_mb=app_data.get("memoryperexecutor", 0),
                    executors=1,  # Default for active apps
                )
            )

        # Process completed applications
        for app_data in data.get("completedapps", []):
            # Convert timestamp if it's a number
            start_time = app_data.get("starttime", 0)
            if isinstance(start_time, (int, float)):
                from datetime import datetime

                start_time = datetime.fromtimestamp(start_time / 1000).isoformat()
                # Calculate end time from start + duration
                duration_ms = app_data.get("duration", 0)
                end_timestamp = (app_data.get("starttime", 0) + duration_ms) / 1000
                end_time = datetime.fromtimestamp(end_timestamp).isoformat()
            else:
                end_time = "unknown"

            applications.append(
                SparkApplication(
                    app_id=app_data.get("id", "unknown"),
                    app_name=app_data.get("name", "unknown"),
                    user=app_data.get("user", "unknown"),
                    state=app_data.get("state", "COMPLETED"),
                    duration=app_data.get("duration", 0),
                    start_time=str(start_time),
                    end_time=str(end_time),
                    cores_granted=app_data.get("cores", 0),
                    max_cores=app_data.get("cores", 0),
                    memory_per_executor_mb=app_data.get("memoryperexecutor", 0),
                    executors=1,  # Default for completed apps
                )
            )

        return applications

    def _get_current_application_info(self) -> List[SparkApplication]:
        """Get current application info when web UI is not available."""
        raise RuntimeError(
            "Application monitoring requires access to Spark Web UI. "
            "Web UI is not available (likely running in local mode or Web UI is disabled). "
            "Please ensure Spark cluster is running with Web UI enabled."
        )

    def get_current_application_metrics(self) -> Dict[str, Any]:
        """Get detailed metrics for the current application."""
        status_tracker = self.sc.statusTracker()

        # Get job information using correct method names
        active_job_ids = status_tracker.getActiveJobsIds()

        # Get job details for active jobs
        job_details = []
        for job_id in active_job_ids:
            try:
                job_info = status_tracker.getJobInfo(job_id)
                if job_info:
                    job_details.append({"job_id": job_info.jobId, "status": job_info.status})
            except Exception:
                # Skip if job info not available
                pass

        # Get stage information
        active_stage_ids = status_tracker.getActiveStageIds()

        # Get stage details for active stages
        stage_details = []
        for stage_id in active_stage_ids:
            try:
                stage_info = status_tracker.getStageInfo(stage_id)
                if stage_info:
                    stage_details.append(
                        {
                            "stage_id": stage_info.stageId,
                            "name": stage_info.name,
                            "num_tasks": stage_info.numTasks,
                            "num_active_tasks": stage_info.numActiveTasks,
                            "num_completed_tasks": stage_info.numCompletedTasks,
                            "num_failed_tasks": stage_info.numFailedTasks,
                            "current_attempt_id": stage_info.currentAttemptId,
                        }
                    )
            except Exception:
                # Skip if stage info not available
                pass

        # Get basic application information (executor info not directly available in PySpark)
        return {
            "application_id": self.sc.applicationId,
            "application_name": self.sc.appName or "unknown",
            "default_parallelism": self.sc.defaultParallelism,
            "jobs": {
                "active": len(active_job_ids),
                "total": len(job_details),  # Total jobs we found details for
                "details": job_details,
            },
            "stages": {
                "active": len(active_stage_ids),
                "total": len(stage_details),  # Total stages we found details for
                "details": stage_details,
            },
            "executors": {
                "count": 0,  # Not available via StatusTracker API
                "total_cores": self.sc.defaultParallelism,  # Use default parallelism as estimate
                "total_memory_mb": 0,  # Not available via StatusTracker API
                "details": [],  # Not available via StatusTracker API
            },
            "spark_context": {
                "master": self.sc.master,
                "app_id": self.sc.applicationId,
                "start_time": "current_session",
            },
        }

    def display_cluster_summary(self) -> None:
        """Display a formatted cluster summary."""
        summary = self.get_cluster_summary()

        print("=" * 60)
        print("🔥 SPARK CLUSTER SUMMARY")
        print("=" * 60)
        print(f"Cluster URL:     {summary.cluster_url}")
        print(f"Status:          {summary.status}")
        print(f"Workers:         {summary.alive_workers} alive, {summary.dead_workers} dead")
        print()
        print("📊 RESOURCE USAGE:")
        print(f"Cores:           {summary.cores_used}/{summary.cores_total} ({summary.cores_free} free)")
        core_util = (summary.cores_used / summary.cores_total * 100) if summary.cores_total > 0 else 0
        print(f"Core Utilization: {core_util:.1f}%")
        print(
            f"Memory:          {summary.memory_used_gb:.1f}GB/{summary.memory_total_gb:.1f}GB "
            f"({summary.memory_free_gb:.1f}GB free)"
        )
        mem_util = (summary.memory_used_gb / summary.memory_total_gb * 100) if summary.memory_total_gb > 0 else 0
        print(f"Memory Utilization: {mem_util:.1f}%")
        print()
        print("🚀 APPLICATIONS:")
        print(f"Running:         {summary.applications_running}")
        print(f"Completed:       {summary.applications_completed}")
        print("=" * 60)

    def display_worker_details(self) -> None:
        """Display detailed information about worker nodes."""
        workers = self.get_worker_nodes()

        print("=" * 100)
        print("👷 WORKER NODES DETAILS")
        print("=" * 100)
        print(f"{'Worker ID':<15} {'Host':<20} {'State':<10} {'Cores':<10} {'Memory (GB)':<12} {'Executors':<10}")
        print("-" * 100)

        for worker in workers:
            cores_info = f"{worker.cores_used}/{worker.cores}"
            memory_info = f"{worker.memory_used_mb / 1024:.1f}/{worker.memory_mb / 1024:.1f}"
            print(
                f"{worker.worker_id:<15} {worker.host:<20} {worker.state:<10} {cores_info:<10} "
                f"{memory_info:<12} {worker.executors:<10}"
            )

        print("=" * 100)

    def display_application_details(self) -> None:
        """Display detailed information about applications."""
        apps = self.get_applications()

        print("=" * 120)
        print("🚀 SPARK APPLICATIONS")
        print("=" * 120)
        print(f"{'App ID':<20} {'Name':<25} {'User':<10} {'State':<12} {'Cores':<8} {'Memory':<10} {'Executors':<10}")
        print("-" * 120)

        for app in apps:
            memory_gb = f"{app.memory_per_executor_mb / 1024:.1f}GB" if app.memory_per_executor_mb > 0 else "N/A"
            print(
                f"{app.app_id:<20} {app.app_name:<25} {app.user:<10} {app.state:<12} "
                f"{app.cores_granted:<8} {memory_gb:<10} {app.executors:<10}"
            )

        print("=" * 120)


# Convenience functions for quick access
def get_cluster_info() -> ClusterSummary:
    """Quick function to get cluster summary."""
    monitor = SparkClusterMonitor()
    return monitor.get_cluster_summary()


def show_cluster_status() -> None:
    """Quick function to display cluster status."""
    monitor = SparkClusterMonitor()
    monitor.display_cluster_summary()
    print()
    monitor.display_worker_details()
    print()
    monitor.display_application_details()


def get_resource_usage() -> Dict[str, float]:
    """Get current resource usage as percentages."""
    summary = get_cluster_info()
    return {
        "core_utilization_percent": (summary.cores_used / summary.cores_total * 100) if summary.cores_total > 0 else 0,
        "memory_utilization_percent": (
            (summary.memory_used_gb / summary.memory_total_gb * 100) if summary.memory_total_gb > 0 else 0
        ),
        "cores_available": summary.cores_free,
        "memory_available_gb": summary.memory_free_gb,
    }
