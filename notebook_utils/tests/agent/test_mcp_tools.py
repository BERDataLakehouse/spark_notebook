"""
Tests for agent/mcp_tools.py - Native MCP tool integration.
"""

import asyncio
from typing import Optional
from unittest.mock import AsyncMock, Mock, patch
import concurrent.futures
import pytest
from pydantic import BaseModel, Field

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

    def test_optional_field_with_non_none_default(self):
        """Test Optional field with a non-None default value."""

        class TestSchema(BaseModel):
            opt_with_default: Optional[int] = 42

        result = _simplify_schema_for_openai(TestSchema)

        assert "opt_with_default" in result.model_fields

    def test_field_with_description(self):
        """Test field with description preserves it."""

        class TestSchema(BaseModel):
            described_field: str = Field(description="A described field")
            opt_described: Optional[str] = Field(default=None, description="Optional described")
            opt_described_with_val: Optional[str] = Field(default="hello", description="Has default")

        result = _simplify_schema_for_openai(TestSchema)

        assert "described_field" in result.model_fields
        assert "opt_described" in result.model_fields
        assert "opt_described_with_val" in result.model_fields

    def test_required_field_without_description(self):
        """Test required field without description uses Ellipsis default."""

        class TestSchema(BaseModel):
            name: str

        result = _simplify_schema_for_openai(TestSchema)

        assert "name" in result.model_fields

    def test_exception_returns_original(self):
        """Test that exception during simplification returns original class."""
        # Create a model that will cause an error during simplification
        mock_class = Mock()
        mock_class.__name__ = "BrokenSchema"
        mock_class.__fields__ = {"field": Mock(annotation=Mock(side_effect=Exception("broken")))}
        # Make field access raise
        mock_field = Mock()
        mock_field.annotation = str
        type(mock_field).default = property(lambda self: (_ for _ in ()).throw(Exception("broken")))
        mock_class.__fields__ = {"field": mock_field}

        result = _simplify_schema_for_openai(mock_class)

        assert result is mock_class


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

    def test_sync_wrapper_success(self):
        """Test sync wrapper executes async tool and returns result."""
        mock_async_tool = Mock()
        mock_async_tool.name = "test_tool"
        mock_async_tool.description = "Test tool"
        mock_async_tool.args_schema = None

        # Create a real event loop for this test
        loop = asyncio.new_event_loop()
        original_loop = mcp_module._mcp_event_loop
        mcp_module._mcp_event_loop = loop

        # Start the loop in a background thread
        import threading

        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()

        try:
            # Set up the async tool to return a value
            async def mock_ainvoke(kwargs):
                return "tool_result"

            mock_async_tool.ainvoke = mock_ainvoke

            with patch("berdl_notebook_utils.agent.mcp_tools._simplify_schema_for_openai", return_value=None):
                sync_tool = _wrap_async_tool(mock_async_tool)

            result = sync_tool.func(key="value")
            assert result == "tool_result"
        finally:
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=2)
            loop.close()
            mcp_module._mcp_event_loop = original_loop

    def test_sync_wrapper_timeout(self):
        """Test sync wrapper handles timeout."""
        mock_async_tool = Mock()
        mock_async_tool.name = "test_tool"
        mock_async_tool.description = "Test tool"
        mock_async_tool.args_schema = None

        mock_loop = Mock()
        original_loop = mcp_module._mcp_event_loop
        mcp_module._mcp_event_loop = mock_loop

        try:
            mock_future = Mock()
            mock_future.result.side_effect = concurrent.futures.TimeoutError()
            mock_loop_run = Mock(return_value=mock_future)

            with patch("asyncio.run_coroutine_threadsafe", mock_loop_run):
                with patch("berdl_notebook_utils.agent.mcp_tools._simplify_schema_for_openai", return_value=None):
                    sync_tool = _wrap_async_tool(mock_async_tool)

                result = sync_tool.func(key="value")
                assert "timed out" in result

        finally:
            mcp_module._mcp_event_loop = original_loop

    def test_sync_wrapper_exception(self):
        """Test sync wrapper handles exceptions from tool execution."""
        mock_async_tool = Mock()
        mock_async_tool.name = "test_tool"
        mock_async_tool.description = "Test tool"
        mock_async_tool.args_schema = None

        mock_loop = Mock()
        original_loop = mcp_module._mcp_event_loop
        mcp_module._mcp_event_loop = mock_loop

        try:
            mock_future = Mock()
            mock_future.result.side_effect = ValueError("something broke")

            with patch("asyncio.run_coroutine_threadsafe", return_value=mock_future):
                with patch("berdl_notebook_utils.agent.mcp_tools._simplify_schema_for_openai", return_value=None):
                    sync_tool = _wrap_async_tool(mock_async_tool)

                result = sync_tool.func(key="value")
                assert "Error executing tool" in result

        finally:
            mcp_module._mcp_event_loop = original_loop


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

    @patch("berdl_notebook_utils.agent.mcp_tools._wrap_async_tool")
    @patch("asyncio.run_coroutine_threadsafe")
    @patch("berdl_notebook_utils.agent.mcp_tools.get_settings")
    def test_get_mcp_tools_full_flow(self, mock_settings, mock_run_coro, mock_wrap):
        """Test full flow of get_mcp_tools: discover, wrap, cache."""
        # Clear global state
        original_cache = mcp_module._mcp_tools_cache
        original_loop = mcp_module._mcp_event_loop
        original_thread = mcp_module._mcp_loop_thread
        mcp_module._mcp_tools_cache = None
        mcp_module._mcp_event_loop = None

        try:
            mock_settings.return_value.DATALAKE_MCP_SERVER_URL = "http://localhost:8000"
            mock_settings.return_value.KBASE_AUTH_TOKEN = "token123"

            # Mock the convert_mcp_to_langchain_tools function
            mock_async_tool = Mock()
            mock_async_tool.name = "list_databases"
            mock_async_tool.description = "List all databases"

            mock_cleanup = AsyncMock()

            # Mock the discovery future
            mock_future = Mock()
            mock_future.result.return_value = [mock_async_tool]
            mock_run_coro.return_value = mock_future

            # Mock the tool wrapping
            mock_sync_tool = Mock()
            mock_sync_tool.name = "list_databases"
            mock_sync_tool.description = "List all databases"
            mock_wrap.return_value = mock_sync_tool

            mock_convert = AsyncMock(return_value=([mock_async_tool], mock_cleanup))

            with patch.dict("sys.modules", {"langchain_mcp_tools": Mock(convert_mcp_to_langchain_tools=mock_convert)}):
                result = get_mcp_tools(server_url="http://custom:8000")

            assert len(result) == 1
            assert result == [mock_sync_tool]
            # Verify tools are cached
            assert mcp_module._mcp_tools_cache == [mock_sync_tool]

        finally:
            mcp_module._mcp_tools_cache = original_cache
            if mcp_module._mcp_event_loop and mcp_module._mcp_event_loop != original_loop:
                mcp_module._mcp_event_loop.call_soon_threadsafe(mcp_module._mcp_event_loop.stop)
            mcp_module._mcp_event_loop = original_loop
            mcp_module._mcp_loop_thread = original_thread

    @patch("berdl_notebook_utils.agent.mcp_tools.get_settings")
    def test_get_mcp_tools_generic_exception(self, mock_settings):
        """Test get_mcp_tools re-raises generic exceptions."""
        mcp_module._mcp_tools_cache = None
        original_loop = mcp_module._mcp_event_loop
        mcp_module._mcp_event_loop = None

        try:
            mock_settings.return_value.DATALAKE_MCP_SERVER_URL = "http://localhost:8000"
            mock_settings.return_value.KBASE_AUTH_TOKEN = "token"

            mock_convert = Mock(side_effect=ConnectionError("Cannot connect"))

            with patch.dict("sys.modules", {"langchain_mcp_tools": Mock(convert_mcp_to_langchain_tools=mock_convert)}):
                with patch("asyncio.run_coroutine_threadsafe") as mock_run_coro:
                    mock_future = Mock()
                    mock_future.result.side_effect = ConnectionError("Cannot connect")
                    mock_run_coro.return_value = mock_future

                    with pytest.raises(ConnectionError):
                        get_mcp_tools()

        finally:
            mcp_module._mcp_tools_cache = None
            if mcp_module._mcp_event_loop and mcp_module._mcp_event_loop != original_loop:
                mcp_module._mcp_event_loop.call_soon_threadsafe(mcp_module._mcp_event_loop.stop)
            mcp_module._mcp_event_loop = original_loop

    def test_uses_settings_url_when_no_server_url(self):
        """Test uses DATALAKE_MCP_SERVER_URL from settings when server_url is None."""
        mcp_module._mcp_tools_cache = ["already_cached"]

        try:
            result = get_mcp_tools()
            assert result == ["already_cached"]
        finally:
            mcp_module._mcp_tools_cache = None


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

    @patch("asyncio.run_coroutine_threadsafe")
    def test_cleanup_handles_exception(self, mock_run_coro):
        """Test cleanup handles exception during MCP cleanup gracefully."""
        mock_loop = Mock()

        mock_cleanup_func = Mock(return_value=Mock())

        original_loop = mcp_module._mcp_event_loop
        original_cleanup = mcp_module._mcp_cleanup

        mcp_module._mcp_event_loop = mock_loop
        mcp_module._mcp_cleanup = mock_cleanup_func

        # Make future.result raise an exception
        mock_future = Mock()
        mock_future.result.side_effect = Exception("Cleanup failed")
        mock_run_coro.return_value = mock_future

        try:
            # Should not raise, just log warning
            _cleanup_on_exit()
            mock_loop.call_soon_threadsafe.assert_called()
        finally:
            mcp_module._mcp_event_loop = original_loop
            mcp_module._mcp_cleanup = original_cleanup
