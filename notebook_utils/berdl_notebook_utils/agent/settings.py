"""
Agent-specific settings and configuration.

This module defines configuration for the BERDL agent, including LLM provider selection,
model parameters, and feature flags.
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class AgentSettings(BaseSettings):
    """
    Configuration for the BERDL Agent.

    These settings control which LLM provider and model to use, as well as
    agent behavior and capabilities.

    All settings can be overridden via environment variables with the AGENT_ prefix.
    """

    # LLM Provider Configuration
    AGENT_MODEL_PROVIDER: Literal["anthropic", "openai", "ollama"] = Field(
        default="openai",
        description="LLM provider to use for the agent (anthropic, openai, or ollama)",
    )

    AGENT_MODEL_NAME: str = Field(
        default="gpt-4o-mini",
        description=(
            "Specific model name. "
            "Anthropic: claude-3-5-sonnet-20241022, claude-3-opus-20240229, etc. "
            "OpenAI: gpt-4o, gpt-4-turbo, gpt-3.5-turbo, etc. "
            "Ollama: llama3, mistral, codellama, etc."
        ),
    )

    AGENT_TEMPERATURE: float = Field(
        default=0.0,
        ge=0.0,
        le=2.0,
        description="LLM temperature (0.0 = deterministic, 2.0 = very creative)",
    )

    AGENT_MAX_ITERATIONS: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum number of reasoning iterations before stopping",
    )

    AGENT_MAX_EXECUTION_TIME: int = Field(
        default=300,
        ge=10,
        le=600,
        description="Maximum execution time in seconds (default: 5 minutes)",
    )

    AGENT_VERBOSE: bool = Field(
        default=True,
        description="Enable verbose logging of agent reasoning steps",
    )

    AGENT_ENABLE_MEMORY: bool = Field(
        default=True,
        description="Enable conversation memory (remembers previous interactions)",
    )

    AGENT_MEMORY_MAX_MESSAGES: int = Field(
        default=20,
        ge=0,
        le=100,
        description="Maximum number of messages to keep in conversation memory (0 = unlimited)",
    )

    # API Keys (optional, can also be set via provider-specific env vars)
    AGENT_ANTHROPIC_API_KEY: str | None = Field(
        default=None,
        alias="ANTHROPIC_API_KEY",
        description="Anthropic API key (reads from ANTHROPIC_API_KEY env var)",
    )

    AGENT_OPENAI_API_KEY: str | None = Field(
        default=None,
        alias="OPENAI_API_KEY",
        description="OpenAI API key (reads from OPENAI_API_KEY env var)",
    )

    AGENT_OLLAMA_BASE_URL: str = Field(
        default="http://localhost:11434",
        alias="OLLAMA_BASE_URL",
        description="Ollama server base URL (for local models)",
    )

    # Tool Configuration
    AGENT_ENABLE_SQL_EXECUTION: bool = Field(
        default=True,
        description="Allow agent to execute SQL queries (disable for metadata-only mode)",
    )

    AGENT_SQL_ROW_LIMIT: int = Field(
        default=1000,
        ge=1,
        le=10000,
        description="Maximum number of rows the agent can query in a single SQL execution",
    )

    model_config = {
        "env_prefix": "AGENT_",
        "case_sensitive": False,
        "extra": "ignore",
    }


def get_agent_settings() -> AgentSettings:
    """
    Get cached AgentSettings instance.

    Returns:
        AgentSettings: Cached settings instance

    Raises:
        ValidationError: If environment variables are invalid
    """
    return AgentSettings()
