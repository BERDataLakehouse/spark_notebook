"""
BERDL Agent - Natural Language Interface to the Data Lake

This module provides an intelligent agent that can interact with the BERDL data lake
using natural language. The agent uses LangChain and LLM providers (Anthropic Claude,
OpenAI GPT, or Ollama) to help users discover, query, and analyze data.

Example:
    >>> from berdl_notebook_utils.agent import create_berdl_agent
    >>> agent = create_berdl_agent()
    >>> result = agent.run("What databases do I have access to?")
    >>> print(result)

The agent has access to the following capabilities:
- Database and table discovery
- Schema inspection
- SQL query generation and execution
- Data sampling and analysis

Configuration is done via environment variables or BERDLSettings:
- AGENT_MODEL_PROVIDER: "anthropic", "openai", or "ollama" (default: "anthropic")
- AGENT_MODEL_NAME: Specific model name (default: "claude-3-5-sonnet-20241022")
- ANTHROPIC_API_KEY: API key for Anthropic models
- OPENAI_API_KEY: API key for OpenAI models
"""

from berdl_notebook_utils.agent.agent import BERDLAgent, create_berdl_agent
from berdl_notebook_utils.agent.settings import AgentSettings, get_agent_settings

__all__ = [
    "create_berdl_agent",
    "BERDLAgent",
    "AgentSettings",
    "get_agent_settings",
]
