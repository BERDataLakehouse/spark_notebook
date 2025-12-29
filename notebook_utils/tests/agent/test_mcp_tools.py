"""
Tests for agent/mcp_tools.py - Native MCP tool integration.
"""

from typing import Optional
from unittest.mock import Mock, patch
import pytest
from pydantic import BaseModel

import berdl_notebook_utils.agent.mcp_tools as mcp_module
from berdl_notebook_utils.agent.mcp_tools import (
    _simplify_schema_for_openai,
    _wrap_async_tool,
    get_mcp_tools,
    clear_mcp_tools_cache,
    cleanup_mcp_tools,
    _run_event_loop,
    _cleanup_on_exit,
)


class TestSimplifySchemaForOpenai:
    """Tests for _simplify_schema_for_openai function."""

    def test_returns_original_if_no_fields(self):
        """Test returns original class if no __fields__ attribute."""

        class SimpleClass:
            pass

        result = _simplify_schema_for_openai(SimpleClass)

        assert result is SimpleClass

    def test_simplifies_optional_fields(self):
        """Test simplifies Optional fields."""

        class TestSchema(BaseModel):
            required_field: str
            optional_field: Optional[str] = None

        result = _simplify_schema_for_openai(TestSchema)

        assert result is not None
        # The simplified model should have both fields
        assert "required_field" in result.model_fields
        assert "optional_field" in result.model_fields


class TestWrapAsyncTool:
    """Tests for _wrap_async_tool function."""

    @patch("berdl_notebook_utils.agent.mcp_tools._mcp_event_loop", new=None)
    def test_sync_wrapper_raises_if_no_loop(self):
        """Test sync wrapper raises if event loop not initialized."""
        mock_async_tool = Mock()
        mock_async_tool.name = "test_tool"
        mock_async_tool.description = "Test tool"
        mock_async_tool.args_schema = None

        with patch("berdl_notebook_utils.agent.mcp_tools._simplify_schema_for_openai", return_value=None):
            sync_tool = _wrap_async_tool(mock_async_tool)

        with pytest.raises(RuntimeError, match="MCP event loop not initialized"):
            sync_tool.func(test_arg="value")


class TestGetMcpTools:
    """Tests for get_mcp_tools function."""

    @patch("berdl_notebook_utils.agent.mcp_tools._mcp_tools_cache", new=["cached_tool"])
    def test_returns_cached_tools(self):
        """Test returns cached tools if available."""
        # Reset the cache to our test value
        mcp_module._mcp_tools_cache = ["cached_tool"]

        result = get_mcp_tools()

        assert result == ["cached_tool"]

        # Clean up
        mcp_module._mcp_tools_cache = None

    @patch("berdl_notebook_utils.agent.mcp_tools.get_settings")
    def test_raises_import_error_if_langchain_mcp_tools_not_installed(self, mock_settings):
        """Test raises ImportError if langchain-mcp-tools not installed."""
        # Clear cache
        mcp_module._mcp_tools_cache = None

        mock_settings.return_value.DATALAKE_MCP_SERVER_URL = "http://localhost:8000"
        mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

        with patch.dict("sys.modules", {"langchain_mcp_tools": None}):
            with patch("builtins.__import__", side_effect=ImportError("No module")):
                with pytest.raises(ImportError, match="langchain-mcp-tools is required"):
                    get_mcp_tools()


class TestClearMcpToolsCache:
    """Tests for clear_mcp_tools_cache function."""

    def test_clears_cache(self):
        """Test clears the tools cache."""
        # Set cache to some value
        mcp_module._mcp_tools_cache = ["some_tool"]

        clear_mcp_tools_cache()

        assert mcp_module._mcp_tools_cache is None


class TestCleanupMcpTools:
    """Tests for cleanup_mcp_tools function."""

    @patch("berdl_notebook_utils.agent.mcp_tools._cleanup_on_exit")
    def test_calls_cleanup_on_exit(self, mock_cleanup):
        """Test calls _cleanup_on_exit."""
        cleanup_mcp_tools()

        mock_cleanup.assert_called_once()


class TestRunEventLoop:
    """Tests for _run_event_loop function."""

    def test_sets_event_loop_and_runs(self):
        """Test sets event loop and runs forever."""
        mock_loop = Mock()

        # Simulate loop running then stopping
        mock_loop.run_forever.side_effect = lambda: None

        with patch("asyncio.set_event_loop") as mock_set_loop:
            _run_event_loop(mock_loop)

            mock_set_loop.assert_called_once_with(mock_loop)
            mock_loop.run_forever.assert_called_once()


class TestCleanupOnExit:
    """Tests for _cleanup_on_exit function."""

    def test_cleanup_when_no_loop(self):
        """Test cleanup does nothing when no event loop."""
        # Ensure no event loop
        original_loop = mcp_module._mcp_event_loop
        mcp_module._mcp_event_loop = None

        try:
            # Should not raise
            _cleanup_on_exit()
        finally:
            mcp_module._mcp_event_loop = original_loop

    @patch("asyncio.run_coroutine_threadsafe")
    def test_cleanup_calls_mcp_cleanup(self, mock_run_coro):
        """Test cleanup calls MCP cleanup function."""
        # Set up mocks
        mock_loop = Mock()

        async def mock_cleanup_coro():
            pass

        mock_cleanup_func = Mock(return_value=mock_cleanup_coro())

        original_loop = mcp_module._mcp_event_loop
        original_cleanup = mcp_module._mcp_cleanup

        mcp_module._mcp_event_loop = mock_loop
        mcp_module._mcp_cleanup = mock_cleanup_func

        mock_future = Mock()
        mock_future.result.return_value = None
        mock_run_coro.return_value = mock_future

        try:
            _cleanup_on_exit()
            mock_loop.call_soon_threadsafe.assert_called()
        finally:
            mcp_module._mcp_event_loop = original_loop
            mcp_module._mcp_cleanup = original_cleanup
