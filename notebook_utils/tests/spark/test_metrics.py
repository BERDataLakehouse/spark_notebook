"""
Tests for spark/metrics.py - Spark job metrics analyzer.
"""

import json
from unittest.mock import Mock, patch

import pytest
import zstandard

from berdl_notebook_utils.spark.metrics import SparkJobMetrics


def _make_obj(name: str) -> Mock:
    """Create a mock MinIO object with the given object_name."""
    obj = Mock()
    obj.object_name = name
    return obj


def _compress_events(events: list[dict]) -> bytes:
    """Compress a list of event dicts into zstd bytes (one JSON per line)."""
    payload = "\n".join(json.dumps(e) for e in events).encode("utf-8")
    cctx = zstandard.ZstdCompressor()
    return cctx.compress(payload)


def _make_minio_response(data: bytes) -> Mock:
    """Create a mock MinIO get_object response."""
    response = Mock()
    response.read.return_value = data
    response.close = Mock()
    response.release_conn = Mock()
    return response


SAMPLE_EVENTS = [
    {
        "Event": "SparkListenerApplicationStart",
        "App ID": "app-20260311022136-0001",
        "App Name": "test-job",
        "Timestamp": 1741651200000,
    },
    {
        "Event": "SparkListenerEnvironmentUpdate",
        "Spark Properties": [["spark.executor.memory", "4g"], ["spark.executor.cores", "2"]],
    },
    {"Event": "SparkListenerExecutorAdded", "Executor ID": "1"},
    {"Event": "SparkListenerStageCompleted", "Stage ID": 0},
    {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": 0,
        "Task Info": {
            "Task ID": 0,
            "Executor ID": "1",
            "Host": "worker-1",
            "Launch Time": 1741651200000,
            "Finish Time": 1741651205000,
        },
        "Task Metrics": {
            "Executor Run Time": 4500,
            "Executor CPU Time": 3000000000,
            "JVM GC Time": 100,
            "Peak Execution Memory": 104857600,
            "Memory Bytes Spilled": 0,
            "Disk Bytes Spilled": 0,
            "Input Metrics": {"Bytes Read": 1048576},
            "Output Metrics": {"Bytes Written": 524288},
            "Shuffle Read Metrics": {"Remote Bytes Read": 0, "Local Bytes Read": 0},
            "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
        },
    },
    {"Event": "SparkListenerApplicationEnd", "Timestamp": 1741651260000},
]


@pytest.fixture
def metrics():
    """Create a SparkJobMetrics instance with mocked MinIO client."""
    with patch("berdl_notebook_utils.spark.metrics.Minio"):
        m = SparkJobMetrics(endpoint="localhost:9000", access_key="test", secret_key="test", secure=False)
    return m


class TestReadEventFiles:
    """Tests for _read_event_files — decompression, parsing, and cleanup."""

    def test_reads_and_parses_zstd_events(self, metrics):
        """Test that zstd-compressed event files are decompressed and parsed."""
        compressed = _compress_events(SAMPLE_EVENTS)
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"

        metrics._client.list_objects.return_value = [
            _make_obj(app_dir + "events_1_spark-job-logs.zstd"),
        ]
        metrics._client.get_object.return_value = _make_minio_response(compressed)

        events = metrics._read_event_files(app_dir)

        assert len(events) == len(SAMPLE_EVENTS)
        assert events[0]["App ID"] == "app-20260311022136-0001"

    def test_skips_non_zstd_files(self, metrics):
        """Test that non-.zstd files are ignored."""
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"
        metrics._client.list_objects.return_value = [
            _make_obj(app_dir + "appstatus_app-20260311.json"),
        ]

        events = metrics._read_event_files(app_dir)
        assert events == []
        metrics._client.get_object.assert_not_called()

    def test_response_closed_on_success(self, metrics):
        """Test that the MinIO response is closed after successful read."""
        compressed = _compress_events([SAMPLE_EVENTS[0]])
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"

        metrics._client.list_objects.return_value = [_make_obj(app_dir + "events.zstd")]
        response = _make_minio_response(compressed)
        metrics._client.get_object.return_value = response

        metrics._read_event_files(app_dir)

        response.close.assert_called_once()
        response.release_conn.assert_called_once()

    def test_response_closed_on_error(self, metrics):
        """Test that the MinIO response is closed even when decompression fails."""
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"
        metrics._client.list_objects.return_value = [_make_obj(app_dir + "events.zstd")]

        response = _make_minio_response(b"not-valid-zstd-data")
        metrics._client.get_object.return_value = response

        events = metrics._read_event_files(app_dir)

        assert events == []
        response.close.assert_called_once()
        response.release_conn.assert_called_once()

    def test_skips_invalid_json_lines(self, metrics):
        """Test that invalid JSON lines are skipped without failing."""
        payload = (
            '{"Event": "SparkListenerApplicationStart", "App ID": "app-1"}\n'
            "not-json\n"
            '{"Event": "SparkListenerApplicationEnd"}\n'
        )
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(payload.encode("utf-8"))

        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"
        metrics._client.list_objects.return_value = [_make_obj(app_dir + "events.zstd")]
        metrics._client.get_object.return_value = _make_minio_response(compressed)

        events = metrics._read_event_files(app_dir)
        assert len(events) == 2


class TestListAllAppDirs:
    """Tests for _list_all_app_dirs — filtering, dedup, sort order, and limit."""

    def test_sorts_by_timestamp_not_username(self, metrics):
        """Test that results are sorted by app timestamp, not username."""
        metrics._client.list_objects.return_value = [
            _make_obj("spark-job-logs/zebra/eventlog_v2_app-20260312000000-0001/events.zstd"),
            _make_obj("spark-job-logs/alice/eventlog_v2_app-20260313000000-0001/events.zstd"),
            _make_obj("spark-job-logs/bob/eventlog_v2_app-20260311000000-0001/events.zstd"),
        ]

        result = metrics._list_all_app_dirs()

        assert result[0][0] == "alice"  # 20260313 — most recent
        assert result[1][0] == "zebra"  # 20260312
        assert result[2][0] == "bob"  # 20260311 — oldest

    def test_deduplicates_multiple_files_per_app(self, metrics):
        """Test that multiple files in the same app dir produce one entry."""
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"
        metrics._client.list_objects.return_value = [
            _make_obj(app_dir + "events_1.zstd"),
            _make_obj(app_dir + "events_2.zstd"),
            _make_obj(app_dir + "appstatus.json"),
        ]

        result = metrics._list_all_app_dirs()
        assert len(result) == 1
        assert result[0] == ("alice", app_dir)

    def test_limit_returns_most_recent(self, metrics):
        """Test that limit returns the N most recent apps globally."""
        metrics._client.list_objects.return_value = [
            _make_obj("spark-job-logs/alice/eventlog_v2_app-20260313000000-0001/events.zstd"),
            _make_obj("spark-job-logs/bob/eventlog_v2_app-20260312000000-0001/events.zstd"),
            _make_obj("spark-job-logs/carol/eventlog_v2_app-20260311000000-0001/events.zstd"),
        ]

        result = metrics._list_all_app_dirs(limit=2)

        assert len(result) == 2
        assert result[0][0] == "alice"
        assert result[1][0] == "bob"

    def test_since_filters_old_dirs(self, metrics):
        """Test that since parameter filters out old app dirs."""
        metrics._client.list_objects.return_value = [
            _make_obj("spark-job-logs/alice/eventlog_v2_app-20260313000000-0001/events.zstd"),
            _make_obj("spark-job-logs/bob/eventlog_v2_app-20260301000000-0001/events.zstd"),
        ]

        result = metrics._list_all_app_dirs(since="20260310")

        assert len(result) == 1
        assert result[0][0] == "alice"

    def test_ignores_non_eventlog_objects(self, metrics):
        """Test that non-eventlog objects are ignored."""
        metrics._client.list_objects.return_value = [
            _make_obj("spark-job-logs/alice/some-other-file.txt"),
            _make_obj("spark-job-logs/alice/eventlog_v2_app-20260311000000-0001/events.zstd"),
        ]

        result = metrics._list_all_app_dirs()
        assert len(result) == 1


class TestEventsToJobMetrics:
    """Tests for _events_to_job_metrics — event parsing into _JobMetrics."""

    def test_parses_all_event_types(self, metrics):
        """Test that all supported event types are parsed correctly."""
        job = metrics._events_to_job_metrics(SAMPLE_EVENTS, "alice")

        assert job.app_id == "app-20260311022136-0001"
        assert job.app_name == "test-job"
        assert job.username == "alice"
        assert job.configured_executor_memory == "4g"
        assert job.configured_executor_cores == 2
        assert job.executor_count == 1
        assert job.total_stages == 1
        assert job.total_tasks == 1
        assert job.total_executor_run_time_ms == 4500
        assert job.total_executor_cpu_time_ns == 3000000000
        assert job.peak_execution_memory_bytes == 104857600
        assert job.total_input_bytes_read == 1048576
        assert job.total_output_bytes_written == 524288
        assert job.start_time is not None
        assert job.end_time is not None

    def test_tracks_peak_memory_across_tasks(self, metrics):
        """Test that peak execution memory tracks the maximum across tasks."""
        events = [
            {"Event": "SparkListenerApplicationStart", "App ID": "app-1"},
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Run Time": 100,
                    "Executor CPU Time": 0,
                    "JVM GC Time": 0,
                    "Peak Execution Memory": 1000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                    "Input Metrics": {},
                    "Output Metrics": {},
                    "Shuffle Read Metrics": {},
                    "Shuffle Write Metrics": {},
                },
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Run Time": 100,
                    "Executor CPU Time": 0,
                    "JVM GC Time": 0,
                    "Peak Execution Memory": 5000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                    "Input Metrics": {},
                    "Output Metrics": {},
                    "Shuffle Read Metrics": {},
                    "Shuffle Write Metrics": {},
                },
            },
            {
                "Event": "SparkListenerTaskEnd",
                "Task Metrics": {
                    "Executor Run Time": 100,
                    "Executor CPU Time": 0,
                    "JVM GC Time": 0,
                    "Peak Execution Memory": 2000,
                    "Memory Bytes Spilled": 0,
                    "Disk Bytes Spilled": 0,
                    "Input Metrics": {},
                    "Output Metrics": {},
                    "Shuffle Read Metrics": {},
                    "Shuffle Write Metrics": {},
                },
            },
        ]
        job = metrics._events_to_job_metrics(events, "alice")

        assert job.peak_execution_memory_bytes == 5000
        assert job.task_peak_memories == [1000, 5000, 2000]
        assert job.total_tasks == 3


class TestGetJobSummary:
    """Tests for get_job_summary — end-to-end with mocked S3."""

    def test_returns_empty_df_when_no_logs(self, metrics):
        """Test that an empty DataFrame is returned when there are no logs."""
        metrics._client.list_objects.return_value = []

        df = metrics.get_job_summary(username="alice")
        assert df.empty

    def test_single_user_returns_summary_row(self, metrics):
        """Test that a single-user query returns a summary DataFrame."""
        compressed = _compress_events(SAMPLE_EVENTS)
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"

        # First call: list app dirs for user; second call: list files in app dir
        metrics._client.list_objects.side_effect = [
            [_make_obj(app_dir)],
            [_make_obj(app_dir + "events.zstd")],
        ]
        metrics._client.get_object.return_value = _make_minio_response(compressed)

        df = metrics.get_job_summary(username="alice")

        assert len(df) == 1
        assert df.iloc[0]["app_id"] == "app-20260311022136-0001"
        assert df.iloc[0]["username"] == "alice"
        assert df.iloc[0]["total_tasks"] == 1
        assert df.iloc[0]["cfg_executor_memory"] == "4g"


class TestGetTaskDetail:
    """Tests for get_task_detail — per-task DataFrame."""

    def test_returns_task_rows(self, metrics):
        """Test that task detail returns one row per task."""
        compressed = _compress_events(SAMPLE_EVENTS)
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"

        metrics._client.list_objects.side_effect = [
            [_make_obj(app_dir)],
            [_make_obj(app_dir + "events.zstd")],
        ]
        metrics._client.get_object.return_value = _make_minio_response(compressed)

        df = metrics.get_task_detail(username="alice")

        assert len(df) == 1
        assert df.iloc[0]["app_id"] == "app-20260311022136-0001"
        assert df.iloc[0]["executor_run_time_ms"] == 4500
        assert df.iloc[0]["peak_execution_memory_mb"] == pytest.approx(100.0, abs=0.1)

    def test_filters_by_app_id(self, metrics):
        """Test that app_id filter excludes non-matching apps."""
        compressed = _compress_events(SAMPLE_EVENTS)
        app_dir = "spark-job-logs/alice/eventlog_v2_app-20260311022136-0001/"

        metrics._client.list_objects.side_effect = [
            [_make_obj(app_dir)],
            [_make_obj(app_dir + "events.zstd")],
        ]
        metrics._client.get_object.return_value = _make_minio_response(compressed)

        df = metrics.get_task_detail(username="alice", app_id="app-nonexistent")

        assert df.empty


class TestDaysToCutoff:
    """Tests for _days_to_cutoff static method."""

    def test_none_returns_none(self):
        assert SparkJobMetrics._days_to_cutoff(None) is None

    def test_returns_date_string(self):
        result = SparkJobMetrics._days_to_cutoff(7)
        assert len(result) == 8
        assert result.isdigit()


class TestListUsers:
    """Tests for list_users."""

    def test_returns_sorted_usernames(self, metrics):
        metrics._client.list_objects.return_value = [
            _make_obj("spark-job-logs/charlie/"),
            _make_obj("spark-job-logs/alice/"),
            _make_obj("spark-job-logs/bob/"),
        ]

        users = metrics.list_users()
        assert users == ["alice", "bob", "charlie"]
