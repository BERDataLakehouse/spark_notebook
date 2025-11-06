"""
Native MCP tool integration for LangChain agent.

This module uses langchain-mcp-tools to automatically discover and load tools
from the running BERDL MCP server (FastApiMCP), eliminating the need to manually
wrap each tool function.

IMPORTANT: The MCP connection must remain open for tools to work. This module
maintains a persistent event loop in a background thread to keep connections alive.
"""

import asyncio
import atexit
import logging
import threading
import traceback
from typing import Any, Union, get_args, get_origin

from langchain_core.tools import StructuredTool
from pydantic import Field, create_model

from berdl_notebook_utils import get_settings

logger = logging.getLogger(__name__)

# Global state for persistent MCP connection
_mcp_event_loop: asyncio.AbstractEventLoop | None = None
_mcp_loop_thread: threading.Thread | None = None
_mcp_cleanup = None
_mcp_tools_cache: list[Any] | None = None


def _run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Run event loop in background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


def _cleanup_on_exit() -> None:
    """Clean up MCP connections on program exit."""
    global _mcp_event_loop, _mcp_cleanup

    if _mcp_cleanup and _mcp_event_loop:
        logger.info("Cleaning up MCP connections on exit...")
        try:
            # Schedule cleanup in the event loop
            future = asyncio.run_coroutine_threadsafe(_mcp_cleanup(), _mcp_event_loop)
            future.result(timeout=5)
            logger.info("MCP cleanup completed")
        except Exception as e:
            logger.warning(f"Error during MCP cleanup: {e}")

    if _mcp_event_loop:
        _mcp_event_loop.call_soon_threadsafe(_mcp_event_loop.stop)


# Register cleanup on exit
atexit.register(_cleanup_on_exit)


def _simplify_schema_for_openai(schema_class: Any) -> Any:
    """
    Simplify Pydantic schema to be OpenAI-compatible.

    Removes 'anyOf' constructs that OpenAI's function calling doesn't handle well.
    For optional parameters with 'anyOf: [type, null]', we keep just the primary type
    and ensure defaults are set properly.

    Args:
        schema_class: The Pydantic model class to simplify

    Returns:
        Simplified Pydantic model class
    """
    try:
        # Get the original schema
        if not hasattr(schema_class, "__fields__"):
            return schema_class

        # Build new field definitions
        new_fields = {}
        for field_name, field in schema_class.__fields__.items():
            field_type = field.annotation
            default = field.default

            # Handle Union types (including Optional which is Union[T, None])
            origin = get_origin(field_type)
            if origin is Union:
                args = get_args(field_type)
                # Get the non-None type(s)
                non_none_types = [arg for arg in args if arg is not type(None)]
                if non_none_types:
                    # Use the first non-None type
                    field_type = non_none_types[0]
                    # Ensure we have a default for optional fields
                    if default is None or (hasattr(field, "default") and field.default is not None):
                        default = field.default
                    elif not hasattr(field, "default") or field.default is None:
                        default = None

            # Create Field with description if available
            description = getattr(field.field_info, "description", None) if hasattr(field, "field_info") else None

            if description:
                if default is not None and default is not ...:
                    new_fields[field_name] = (field_type, Field(default=default, description=description))
                elif default is None:
                    new_fields[field_name] = (field_type, Field(default=None, description=description))
                else:
                    new_fields[field_name] = (field_type, Field(description=description))
            else:
                if default is not None and default is not ...:
                    new_fields[field_name] = (field_type, default)
                elif default is None:
                    new_fields[field_name] = (field_type, None)
                else:
                    new_fields[field_name] = (field_type, ...)

        # Create a new simplified model
        simplified_model = create_model(schema_class.__name__, **new_fields)

        logger.debug(f"Simplified schema for {schema_class.__name__}")
        return simplified_model

    except Exception as e:
        logger.warning(f"Could not simplify schema for {schema_class.__name__}: {e}")
        return schema_class


def _wrap_async_tool(async_tool: Any) -> StructuredTool:
    """
    Wrap an async MCP tool to work with synchronous LangChain agents.

    This creates a sync wrapper that runs the async tool in the persistent event loop.

    Args:
        async_tool: The async MCP tool to wrap

    Returns:
        Synchronous StructuredTool that can be used with LangChain
    """

    # Create sync wrapper function
    def sync_func(**kwargs):
        """Sync wrapper that executes async tool in event loop."""
        global _mcp_event_loop

        if _mcp_event_loop is None:
            raise RuntimeError("MCP event loop not initialized")

        # Debug logging
        logger.debug(f"Tool {async_tool.name} called with kwargs: {kwargs}")
        logger.debug(f"  kwargs type: {type(kwargs)}")
        logger.debug(f"  kwargs keys: {list(kwargs.keys()) if isinstance(kwargs, dict) else 'N/A'}")

        # Schedule the async invocation in the persistent loop
        future = asyncio.run_coroutine_threadsafe(async_tool.ainvoke(kwargs), _mcp_event_loop)

        # Wait for result
        try:
            result = future.result(timeout=120)  # 2 minute timeout for queries
            return result
        except TimeoutError:
            return "Error: Tool execution timed out after 2 minutes"
        except Exception as e:
            logger.error(f"Error executing tool {async_tool.name}: {e}")
            logger.debug(f"  Exception type: {type(e)}")
            return f"Error executing tool: {e}"

    # Simplify the schema for OpenAI compatibility
    simplified_schema = _simplify_schema_for_openai(async_tool.args_schema)

    # Create new sync tool with same metadata
    return StructuredTool(
        name=async_tool.name,
        description=async_tool.description,
        func=sync_func,
        args_schema=simplified_schema,
    )


def get_mcp_tools(server_url: str | None = None) -> list[Any]:
    """
    Load tools from the BERDL MCP server using langchain-mcp-tools.

    This connects to the datalake-mcp-server (FastApiMCP) and maintains
    a persistent connection in a background thread. The connection stays
    alive for the lifetime of the process, allowing tools to work correctly.

    Args:
        server_url: MCP server URL (defaults from settings)

    Returns:
        List of LangChain Tool objects auto-discovered from MCP server

    Raises:
        ImportError: If langchain-mcp-tools is not installed
        Exception: If MCP server is unreachable or tool discovery fails
    """
    global _mcp_event_loop, _mcp_loop_thread, _mcp_cleanup, _mcp_tools_cache

    # Return cached tools if already loaded
    if _mcp_tools_cache is not None:
        logger.debug("Returning cached MCP tools")
        return _mcp_tools_cache

    # Get settings
    berdl_settings = get_settings()

    # Use provided URL or get from settings
    if not server_url:
        server_url = str(berdl_settings.DATALAKE_MCP_SERVER_URL)

    # Get KBase auth token from settings
    auth_token = berdl_settings.KBASE_AUTH_TOKEN

    logger.info(f"Discovering MCP tools from server: {server_url}")

    try:
        # Import here to avoid dependency issues if package not installed
        from langchain_mcp_tools import convert_mcp_to_langchain_tools

        # Configure MCP server connection with authentication
        mcp_servers = {"datalake": {"url": server_url, "headers": {"Authorization": f"Bearer {auth_token}"}}}

        # Create persistent event loop if not exists
        if _mcp_event_loop is None:
            _mcp_event_loop = asyncio.new_event_loop()
            _mcp_loop_thread = threading.Thread(
                target=_run_event_loop, args=(_mcp_event_loop,), daemon=True, name="MCP-EventLoop"
            )
            _mcp_loop_thread.start()
            logger.info("Started persistent MCP event loop in background thread")

        # Discover tools in the persistent event loop
        async def _discover_tools():
            global _mcp_cleanup
            tools, cleanup = await convert_mcp_to_langchain_tools(mcp_servers)
            _mcp_cleanup = cleanup
            return tools

        # Execute discovery in persistent loop
        future = asyncio.run_coroutine_threadsafe(_discover_tools(), _mcp_event_loop)
        tools = future.result(timeout=30)

        # Wrap async tools as sync tools for LangChain compatibility
        sync_tools = [_wrap_async_tool(tool) for tool in tools]

        # Cache the sync-wrapped tools
        _mcp_tools_cache = sync_tools

        logger.info(f"Successfully discovered {len(sync_tools)} tools from MCP server")
        logger.info("MCP connection maintained in background thread for tool execution")
        for tool in sync_tools:
            logger.debug(f"  - {tool.name}: {tool.description[:100] if hasattr(tool, 'description') else ''}")

        return sync_tools

    except ImportError as e:
        logger.error("langchain-mcp-tools not installed. Run: uv sync --group dev")
        raise ImportError(
            "langchain-mcp-tools is required for MCP integration. Install with: uv sync --group dev"
        ) from e
    except Exception as e:
        logger.error(f"Failed to discover MCP tools from {server_url}: {e}")
        traceback.print_exc()
        raise


def clear_mcp_tools_cache() -> None:
    """
    Clear the cached MCP tools to force reloading on next get_mcp_tools() call.

    This is useful when you want to reload tools after making changes to the schema
    processing or tool definitions.
    """
    global _mcp_tools_cache
    _mcp_tools_cache = None
    logger.info("MCP tools cache cleared")


def cleanup_mcp_tools() -> None:
    """
    Manually clean up MCP connections.

    Note: Cleanup is also automatically performed on program exit via atexit.
    """
    _cleanup_on_exit()
