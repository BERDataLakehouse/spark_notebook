"""
Spark Job Metrics Analyzer

Reads Spark event logs directly from MinIO (S3) to extract resource utilization
metrics across all users and jobs. Useful for capacity planning and determining
optimal cluster sizing.

Defaults to MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, MINIO_SECRET_KEY from env.
With user creds, can only read own logs. Pass admin creds to read all users.

Usage::

    from berdl_notebook_utils.spark.metrics import SparkJobMetrics

    # Own logs (env creds)
    metrics = SparkJobMetrics()
    df = metrics.get_job_summary()

    # All users (admin creds)
    metrics = SparkJobMetrics(access_key="admin-key", secret_key="admin-secret")
    df = metrics.get_job_summary()

    # Filtered queries
    df = metrics.get_job_summary(username="alice", days=7, limit=10)
    tasks = metrics.get_task_detail(username="alice", days=3)

    # Quick stats
    print(f"P95 peak memory: {tasks['peak_execution_memory_mb'].quantile(0.95):.1f} MB")
    print(f"Mean CPU efficiency: {df['cpu_efficiency_pct'].mean():.1f}%")
"""

import io
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pandas as pd
import zstandard
from minio import Minio

logger = logging.getLogger(__name__)

BUCKET = "cdm-spark-job-logs"
PREFIX = "spark-job-logs/"


@dataclass
class _JobMetrics:
    """Accumulated metrics for a single Spark application."""

    app_id: str = ""
    app_name: str = ""
    username: str = ""
    start_time: datetime | None = None
    end_time: datetime | None = None

    # Configured resources (from SparkListenerEnvironmentUpdate)
    configured_executor_memory: str = ""
    configured_executor_cores: int = 0
    configured_driver_memory: str = ""
    configured_driver_cores: int = 0
    configured_max_cores: int = 0
    configured_executor_instances: int = 0
    configured_dynamic_allocation: bool = False

    # Allocated resources (from SparkListenerResourceProfileAdded)
    allocated_executor_memory_mb: int = 0
    allocated_executor_cores: int = 0

    # Actual JVM memory (from SparkListenerBlockManagerAdded)
    block_manager_max_memory_bytes: int = 0
    block_manager_max_onheap_bytes: int = 0

    # Executor tracking
    executor_count: int = 0

    # Aggregated task metrics (from SparkListenerTaskEnd)
    total_tasks: int = 0
    total_executor_run_time_ms: int = 0
    total_executor_cpu_time_ns: int = 0
    total_gc_time_ms: int = 0
    peak_execution_memory_bytes: int = 0
    total_memory_spilled_bytes: int = 0
    total_disk_spilled_bytes: int = 0
    total_input_bytes_read: int = 0
    total_output_bytes_written: int = 0
    total_shuffle_read_bytes: int = 0
    total_shuffle_write_bytes: int = 0

    # Stage tracking
    total_stages: int = 0

    # Per-task peak memory samples for distribution analysis
    task_peak_memories: list[int] = field(default_factory=list)


class SparkJobMetrics:
    """Reads Spark event logs from MinIO and produces resource utilization DataFrames."""

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        secure: bool | None = None,
        bucket: str = BUCKET,
        prefix: str = PREFIX,
    ):
        ep = endpoint or os.environ.get("MINIO_ENDPOINT_URL", "")
        access_key = access_key or os.environ.get("MINIO_ACCESS_KEY", "")
        secret_key = secret_key or os.environ.get("MINIO_SECRET_KEY", "")

        # Strip protocol prefix and infer secure if not explicitly set
        for scheme in ("https://", "http://"):
            if ep.startswith(scheme):
                ep = ep[len(scheme) :]
                if secure is None:
                    secure = scheme == "https://"
                break

        if secure is None:
            secure = os.environ.get("MINIO_SECURE", "false").lower() in ("true", "1", "yes")

        self._client = Minio(ep, access_key=access_key, secret_key=secret_key, secure=secure)
        self._bucket = bucket
        self._prefix = prefix

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_users(self) -> list[str]:
        """List all usernames that have event logs."""
        users = set()
        for obj in self._client.list_objects(self._bucket, prefix=self._prefix, recursive=False):
            name = obj.object_name.removeprefix(self._prefix).strip("/")
            if name:
                users.add(name)
        return sorted(users)

    def get_job_summary(
        self,
        username: str | None = None,
        days: int | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """
        Return a DataFrame with one row per Spark application.

        Columns include configured resources, actual peak usage, CPU efficiency,
        memory efficiency, spill ratios, and duration.

        Args:
            username: Filter to a single user. None = all users.
            days: Only include logs from the last N days. None = all time.
            limit: Max number of most recent apps to return. None = all.
        """
        since = self._days_to_cutoff(days)

        if username:
            # Single-user: per-user listing (fast for one user)
            jobs = self._parse_user_logs(username, since=since, limit=limit)
        else:
            # All-users: single recursive S3 list (1 call instead of 70+)
            app_entries = self._list_all_app_dirs(since=since, limit=limit)
            jobs = []
            for user, app_dir in app_entries:
                events = self._read_event_files(app_dir)
                if not events:
                    continue
                job = self._events_to_job_metrics(events, user)
                if job.app_id:
                    jobs.append(job)

        if not jobs:
            return pd.DataFrame()

        return self._jobs_to_summary_df(jobs)

    def get_task_detail(
        self,
        username: str | None = None,
        app_id: str | None = None,
        days: int | None = None,
    ) -> pd.DataFrame:
        """
        Return a DataFrame with one row per task.

        Useful for identifying outlier tasks that consume disproportionate resources.

        Args:
            username: Filter to a single user. None = all users.
            app_id: Filter to a single application. None = all apps.
            days: Only include logs from the last N days. None = all time.
        """
        since = self._days_to_cutoff(days)

        if username:
            users_and_dirs = [(username, d) for d in self._list_app_dirs(username, since=since)]
        else:
            users_and_dirs = self._list_all_app_dirs(since=since)

        rows: list[dict] = []
        for user, app_dir in users_and_dirs:
            events = self._read_event_files(app_dir)
            if not events:
                continue

            app_id_found = ""
            app_name = ""
            for event in events:
                if event.get("Event") == "SparkListenerApplicationStart":
                    app_id_found = event.get("App ID", "")
                    app_name = event.get("App Name", "")
                    break

            if app_id and app_id_found != app_id:
                continue

            for event in events:
                if event.get("Event") != "SparkListenerTaskEnd":
                    continue

                task_metrics = event.get("Task Metrics", {})
                task_info = event.get("Task Info", {})
                if not task_metrics:
                    continue

                rows.append(
                    {
                        "username": user,
                        "app_id": app_id_found,
                        "app_name": app_name,
                        "stage_id": event.get("Stage ID"),
                        "task_id": task_info.get("Task ID"),
                        "executor_id": task_info.get("Executor ID"),
                        "host": task_info.get("Host"),
                        "duration_ms": task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0),
                        "executor_run_time_ms": task_metrics.get("Executor Run Time", 0),
                        "executor_cpu_time_ms": task_metrics.get("Executor CPU Time", 0) / 1_000_000,
                        "gc_time_ms": task_metrics.get("JVM GC Time", 0),
                        "peak_execution_memory_mb": task_metrics.get("Peak Execution Memory", 0) / (1024 * 1024),
                        "memory_spilled_mb": task_metrics.get("Memory Bytes Spilled", 0) / (1024 * 1024),
                        "disk_spilled_mb": task_metrics.get("Disk Bytes Spilled", 0) / (1024 * 1024),
                        "input_bytes_read": task_metrics.get("Input Metrics", {}).get("Bytes Read", 0),
                        "output_bytes_written": task_metrics.get("Output Metrics", {}).get("Bytes Written", 0),
                        "shuffle_read_bytes": (
                            task_metrics.get("Shuffle Read Metrics", {}).get("Remote Bytes Read", 0)
                            + task_metrics.get("Shuffle Read Metrics", {}).get("Local Bytes Read", 0)
                        ),
                        "shuffle_write_bytes": task_metrics.get("Shuffle Write Metrics", {}).get(
                            "Shuffle Bytes Written", 0
                        ),
                    }
                )

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Internal: S3 listing and reading
    # ------------------------------------------------------------------

    @staticmethod
    def _days_to_cutoff(days: int | None) -> str | None:
        """Convert days-ago to a timestamp prefix string for filtering app dirs.

        App dirs are named like: eventlog_v2_app-20260311022136-0003
        The timestamp portion is YYYYMMDDHHmmss. We generate a cutoff
        string like '20260308' so we can skip dirs older than that.
        """
        if days is None:
            return None
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
        return cutoff.strftime("%Y%m%d")

    def _list_all_app_dirs(self, since: str | None = None, limit: int | None = None) -> list[tuple[str, str]]:
        """List eventlog_v2_* dirs across ALL users with a single recursive S3 call.

        Returns (username, app_dir_prefix) tuples sorted by timestamp descending.
        Much faster than per-user listing when querying all users (~1 S3 call vs 70+).
        """
        dirs: list[tuple[str, str]] = []
        for obj in self._client.list_objects(self._bucket, prefix=self._prefix, recursive=True):
            name = obj.object_name or ""
            if "eventlog_v2_" not in name:
                continue

            # Extract username from path: spark-job-logs/{username}/eventlog_v2_.../file
            rel = name.removeprefix(self._prefix)
            parts = rel.split("/")
            if len(parts) < 2:
                continue
            username = parts[0]

            # Extract app dir (up to and including the eventlog_v2_* segment)
            app_dir = self._prefix + username + "/" + parts[1] + "/"

            if since:
                try:
                    ts_str = parts[1].split("eventlog_v2_app-")[1][:8]
                    if ts_str < since:
                        continue
                except (IndexError, ValueError):
                    pass

            dirs.append((username, app_dir))

        # Deduplicate (multiple files per app dir) and sort descending
        seen: set[str] = set()
        unique: list[tuple[str, str]] = []
        for username, app_dir in dirs:
            if app_dir not in seen:
                seen.add(app_dir)
                unique.append((username, app_dir))

        def _extract_ts(entry: tuple[str, str]) -> str:
            """Extract timestamp from app dir for sorting (e.g. '20260311022136-0003')."""
            try:
                return entry[1].split("eventlog_v2_app-")[1].split("/")[0]
            except (IndexError, ValueError):
                return ""

        unique.sort(key=_extract_ts, reverse=True)

        if limit:
            unique = unique[:limit]

        return unique

    def _list_app_dirs(self, username: str, since: str | None = None, limit: int | None = None) -> list[str]:
        """List eventlog_v2_* directories for a user, optionally filtered by date.

        Args:
            since: Date cutoff string like '20260308'. Dirs with timestamps
                   before this are skipped.
            limit: Return only the N most recent app dirs (by timestamp in name).
        """
        user_prefix = f"{self._prefix}{username}/"
        dirs = []
        for obj in self._client.list_objects(self._bucket, prefix=user_prefix, recursive=False):
            name = obj.object_name or ""
            if "eventlog_v2_" not in name:
                continue
            if since:
                # Extract timestamp from: ...eventlog_v2_app-20260311022136-0003/
                try:
                    app_part = name.split("eventlog_v2_app-")[1]
                    ts_str = app_part[:8]  # YYYYMMDD
                    if ts_str < since:
                        continue
                except (IndexError, ValueError):
                    pass
            dirs.append(name)

        if limit:
            # Sort descending by name (timestamp is embedded) and take N most recent
            dirs.sort(reverse=True)
            dirs = dirs[:limit]

        return dirs

    def _read_event_files(self, app_dir: str) -> list[dict]:
        """Read and decompress all events_*.zstd files in an app directory."""
        events = []
        for obj in self._client.list_objects(self._bucket, prefix=app_dir, recursive=False):
            if not obj.object_name.endswith(".zstd"):
                continue
            object_name = obj.object_name or ""
            response = None
            try:
                response = self._client.get_object(self._bucket, object_name)
                compressed = response.read()
                dctx = zstandard.ZstdDecompressor()
                reader = dctx.stream_reader(io.BytesIO(compressed))
                text_stream = io.TextIOWrapper(reader, encoding="utf-8")
                for line in text_stream:
                    line = line.strip()
                    if line:
                        try:
                            events.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                logger.warning(f"Failed to read {obj.object_name}: {e}")
            finally:
                if response is not None:
                    response.close()
                    response.release_conn()
        return events

    # ------------------------------------------------------------------
    # Internal: Event parsing
    # ------------------------------------------------------------------

    def _parse_user_logs(self, username: str, since: str | None = None, limit: int | None = None) -> list[_JobMetrics]:
        """Parse all event logs for a user into JobMetrics objects."""
        app_dirs = self._list_app_dirs(username, since=since, limit=limit)
        jobs = []
        for app_dir in app_dirs:
            events = self._read_event_files(app_dir)
            if not events:
                continue
            job = self._events_to_job_metrics(events, username)
            if job.app_id:
                jobs.append(job)
        return jobs

    def _events_to_job_metrics(self, events: list[dict], username: str) -> _JobMetrics:
        """Convert a list of events into a single JobMetrics."""
        job = _JobMetrics(username=username)

        for event in events:
            event_type = event.get("Event", "")

            if event_type == "SparkListenerApplicationStart":
                job.app_id = event.get("App ID", "")
                job.app_name = event.get("App Name", "")
                ts = event.get("Timestamp")
                if ts:
                    job.start_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

            elif event_type == "SparkListenerApplicationEnd":
                ts = event.get("Timestamp")
                if ts:
                    job.end_time = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)

            elif event_type == "SparkListenerEnvironmentUpdate":
                props = dict(event.get("Spark Properties", []))
                job.configured_executor_memory = props.get("spark.executor.memory", "")
                job.configured_executor_cores = int(props.get("spark.executor.cores", 0))
                job.configured_driver_memory = props.get("spark.driver.memory", "")
                job.configured_driver_cores = int(props.get("spark.driver.cores", 0))
                job.configured_max_cores = int(props.get("spark.cores.max", 0))
                job.configured_executor_instances = int(props.get("spark.executor.instances", 0))
                job.configured_dynamic_allocation = props.get("spark.dynamicAllocation.enabled", "false") == "true"

            elif event_type == "SparkListenerResourceProfileAdded":
                exec_reqs = event.get("Executor Resource Requests", {})
                if "memory" in exec_reqs:
                    job.allocated_executor_memory_mb = exec_reqs["memory"].get("Amount", 0)
                if "cores" in exec_reqs:
                    job.allocated_executor_cores = exec_reqs["cores"].get("Amount", 0)

            elif event_type == "SparkListenerBlockManagerAdded":
                max_mem = event.get("Maximum Memory", 0)
                if max_mem > job.block_manager_max_memory_bytes:
                    job.block_manager_max_memory_bytes = max_mem
                max_onheap = event.get("Maximum Onheap Memory", 0)
                if max_onheap > job.block_manager_max_onheap_bytes:
                    job.block_manager_max_onheap_bytes = max_onheap

            elif event_type == "SparkListenerExecutorAdded":
                job.executor_count += 1

            elif event_type == "SparkListenerStageCompleted":
                job.total_stages += 1

            elif event_type == "SparkListenerTaskEnd":
                task_metrics = event.get("Task Metrics", {})
                if not task_metrics:
                    continue

                job.total_tasks += 1
                job.total_executor_run_time_ms += task_metrics.get("Executor Run Time", 0)
                job.total_executor_cpu_time_ns += task_metrics.get("Executor CPU Time", 0)
                job.total_gc_time_ms += task_metrics.get("JVM GC Time", 0)
                job.total_memory_spilled_bytes += task_metrics.get("Memory Bytes Spilled", 0)
                job.total_disk_spilled_bytes += task_metrics.get("Disk Bytes Spilled", 0)

                peak_mem = task_metrics.get("Peak Execution Memory", 0)
                if peak_mem > job.peak_execution_memory_bytes:
                    job.peak_execution_memory_bytes = peak_mem
                job.task_peak_memories.append(peak_mem)

                input_metrics = task_metrics.get("Input Metrics", {})
                job.total_input_bytes_read += input_metrics.get("Bytes Read", 0)

                output_metrics = task_metrics.get("Output Metrics", {})
                job.total_output_bytes_written += output_metrics.get("Bytes Written", 0)

                shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
                job.total_shuffle_read_bytes += shuffle_read.get("Remote Bytes Read", 0)
                job.total_shuffle_read_bytes += shuffle_read.get("Local Bytes Read", 0)

                shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
                job.total_shuffle_write_bytes += shuffle_write.get("Shuffle Bytes Written", 0)

        return job

    # ------------------------------------------------------------------
    # Internal: DataFrame construction
    # ------------------------------------------------------------------

    @staticmethod
    def _jobs_to_summary_df(jobs: list[_JobMetrics]) -> pd.DataFrame:
        """Convert JobMetrics list to a summary DataFrame."""
        rows = []
        for j in jobs:
            duration_sec = None
            if j.start_time and j.end_time:
                duration_sec = (j.end_time - j.start_time).total_seconds()

            total_cpu_time_ms = j.total_executor_cpu_time_ns / 1_000_000
            cpu_efficiency = None
            if j.total_executor_run_time_ms > 0:
                cpu_efficiency = round(total_cpu_time_ms / j.total_executor_run_time_ms * 100, 1)

            avg_task_peak_mb = 0.0
            p95_task_peak_mb = 0.0
            if j.task_peak_memories:
                sorted_peaks = sorted(j.task_peak_memories)
                avg_task_peak_mb = sum(sorted_peaks) / len(sorted_peaks) / (1024 * 1024)
                p95_idx = int(len(sorted_peaks) * 0.95)
                p95_task_peak_mb = sorted_peaks[min(p95_idx, len(sorted_peaks) - 1)] / (1024 * 1024)

            rows.append(
                {
                    "username": j.username,
                    "app_id": j.app_id,
                    "app_name": j.app_name,
                    "start_time": j.start_time,
                    "duration_sec": duration_sec,
                    # Configured
                    "cfg_executor_memory": j.configured_executor_memory,
                    "cfg_executor_cores": j.configured_executor_cores,
                    "cfg_driver_memory": j.configured_driver_memory,
                    "cfg_max_cores": j.configured_max_cores,
                    "cfg_executor_instances": j.configured_executor_instances,
                    "cfg_dynamic_allocation": j.configured_dynamic_allocation,
                    # Allocated
                    "alloc_executor_memory_mb": j.allocated_executor_memory_mb,
                    "alloc_executor_cores": j.allocated_executor_cores,
                    "executor_count": j.executor_count,
                    "block_mgr_max_memory_mb": round(j.block_manager_max_memory_bytes / (1024 * 1024), 1),
                    # Usage
                    "total_tasks": j.total_tasks,
                    "total_stages": j.total_stages,
                    "total_cpu_time_sec": round(total_cpu_time_ms / 1000, 2),
                    "total_executor_run_time_sec": round(j.total_executor_run_time_ms / 1000, 2),
                    "cpu_efficiency_pct": cpu_efficiency,
                    "total_gc_time_sec": round(j.total_gc_time_ms / 1000, 2),
                    "gc_ratio_pct": (
                        round(j.total_gc_time_ms / j.total_executor_run_time_ms * 100, 1)
                        if j.total_executor_run_time_ms > 0
                        else None
                    ),
                    "peak_execution_memory_mb": round(j.peak_execution_memory_bytes / (1024 * 1024), 1),
                    "avg_task_peak_memory_mb": round(avg_task_peak_mb, 1),
                    "p95_task_peak_memory_mb": round(p95_task_peak_mb, 1),
                    "memory_spilled_mb": round(j.total_memory_spilled_bytes / (1024 * 1024), 1),
                    "disk_spilled_mb": round(j.total_disk_spilled_bytes / (1024 * 1024), 1),
                    "input_read_mb": round(j.total_input_bytes_read / (1024 * 1024), 1),
                    "output_written_mb": round(j.total_output_bytes_written / (1024 * 1024), 1),
                    "shuffle_read_mb": round(j.total_shuffle_read_bytes / (1024 * 1024), 1),
                    "shuffle_write_mb": round(j.total_shuffle_write_bytes / (1024 * 1024), 1),
                }
            )

        df = pd.DataFrame(rows)
        df = df.sort_values("start_time", ascending=False).reset_index(drop=True)
        return df
