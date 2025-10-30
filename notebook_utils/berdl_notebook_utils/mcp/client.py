"""
Client factory for datalake-mcp-server.

This module provides a factory function to create authenticated clients
for the global datalake-mcp-server.
"""

import logging
from functools import lru_cache

import httpx
from datalake_mcp_server_client.client import AuthenticatedClient

from berdl_notebook_utils.berdl_settings import get_settings

logger = logging.getLogger(__name__)

# Timeout for long-running queries (5 minutes)
DEFAULT_TIMEOUT = 300.0


@lru_cache(maxsize=1)
def get_datalake_mcp_client() -> AuthenticatedClient:
    """
    Get an authenticated client for the datalake-mcp-server.

    This function creates and returns an authenticated client configured to connect
    to the global datalake-mcp-server. The server will use the user's authentication
    token to connect to their personal Spark Connect server.

    The client is cached to avoid recreating it on every call.

    Returns:
        AuthenticatedClient: Configured and authenticated MCP client

    Raises:
        Exception: If required environment variables are missing or invalid

    Example:
        >>> client = get_datalake_mcp_client()
        >>> # Use client for API calls
        >>> from datalake_mcp_server_client.api.delta_lake import list_databases
        >>> response = list_databases.sync(client=client, body=request)
    """
    settings = get_settings()

    logger.info(f"Creating datalake MCP client for server: {settings.DATALAKE_MCP_SERVER_URL}")

    client = AuthenticatedClient(
        base_url=str(settings.DATALAKE_MCP_SERVER_URL),
        token=settings.KBASE_AUTH_TOKEN,
        timeout=httpx.Timeout(DEFAULT_TIMEOUT),
        verify_ssl=True,
    )

    logger.debug("Datalake MCP client created successfully")
    return client

