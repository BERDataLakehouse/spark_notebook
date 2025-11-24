"""
Simple unit tests for environment validation.

Environment variables are set by the pytest_env plugin; see the `tool.pytest_env` section
of pyproject.toml for the environment used in these tests.
"""

import pytest

from berdl_notebook_utils import validate_environment


def test_valid_environment() -> None:
    """Test that all valid environment variables return empty list."""
    result = validate_environment()
    assert result == []


def test_missing_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that missing variables are returned in list."""
    monkeypatch.setenv("BERDL_HIVE_METASTORE_URI", "")
    monkeypatch.delenv("CDM_TASK_SERVICE_URL")
    result = validate_environment()

    assert isinstance(result, list)
    assert set(result) == {"BERDL_HIVE_METASTORE_URI", "CDM_TASK_SERVICE_URL"}
