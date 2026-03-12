"""Tests for agent/prompts.py - System prompt generation."""

from unittest.mock import Mock, patch

from berdl_notebook_utils.agent.prompts import TOOL_DESCRIPTIONS, get_system_prompt


class TestGetSystemPrompt:
    """Tests for get_system_prompt function."""

    def test_with_explicit_username(self):
        """Test prompt generation with an explicit username."""
        prompt = get_system_prompt(username="alice")

        assert "alice" in prompt
        assert "u_alice__" in prompt
        assert "BERDL Data Lake Assistant" in prompt

    def test_with_none_username_auto_detects(self):
        """Test auto-detection of username from settings when None."""
        mock_settings = Mock()
        mock_settings.USER = "bob"

        with patch("berdl_notebook_utils.agent.prompts.get_settings", return_value=mock_settings):
            prompt = get_system_prompt(username=None)

        assert "bob" in prompt
        assert "u_bob__" in prompt

    def test_with_none_username_fallback_on_error(self):
        """Test falls back to 'unknown' when settings raise an exception."""
        with patch("berdl_notebook_utils.agent.prompts.get_settings", side_effect=Exception("no settings")):
            prompt = get_system_prompt(username=None)

        assert "unknown" in prompt
        assert "u_unknown__" in prompt

    def test_prompt_contains_platform_sections(self):
        """Test prompt contains all expected platform sections."""
        prompt = get_system_prompt(username="test_user")

        # Key sections that should be present
        assert "## User Context" in prompt
        assert "## BERDL Platform Architecture" in prompt
        assert "## Data Organization" in prompt
        assert "## Available Tools" in prompt
        assert "## Best Practices" in prompt
        assert "## Example Workflows" in prompt
        assert "## Error Handling" in prompt
        assert "## Important Constraints" in prompt
        assert "## Response Style" in prompt

    def test_prompt_contains_tool_references(self):
        """Test prompt references the available tools."""
        prompt = get_system_prompt(username="test_user")

        assert "list_databases" in prompt
        assert "list_tables" in prompt
        assert "get_table_schema" in prompt
        assert "sample_table" in prompt
        assert "query_table" in prompt

    def test_returns_string(self):
        """Test return type is string."""
        result = get_system_prompt(username="test")
        assert isinstance(result, str)
        assert len(result) > 100  # Non-trivial prompt


class TestToolDescriptions:
    """Tests for TOOL_DESCRIPTIONS constant."""

    def test_contains_expected_tools(self):
        """Test TOOL_DESCRIPTIONS has all expected tool names."""
        expected_tools = [
            "list_databases",
            "list_tables",
            "get_table_schema",
            "get_database_structure",
            "sample_table",
            "count_table_rows",
            "query_table",
        ]
        for tool in expected_tools:
            assert tool in TOOL_DESCRIPTIONS

    def test_descriptions_are_non_empty_strings(self):
        """Test all tool descriptions are non-empty strings."""
        for name, desc in TOOL_DESCRIPTIONS.items():
            assert isinstance(desc, str), f"{name} description is not a string"
            assert len(desc) > 10, f"{name} description is too short"
