"""
Tests for agent/settings.py - Agent settings configuration.
"""

from berdl_notebook_utils.agent.settings import AgentSettings, get_agent_settings


class TestGetAgentSettings:
    """Tests for get_agent_settings function."""

    def test_returns_agent_settings_instance(self):
        """Test get_agent_settings returns an AgentSettings instance."""
        settings = get_agent_settings()
        assert isinstance(settings, AgentSettings)

    def test_default_values(self):
        """Test default settings values."""
        settings = get_agent_settings()
        assert settings.AGENT_MODEL_PROVIDER == "openai"
        assert settings.AGENT_TEMPERATURE == 0.0
        assert settings.AGENT_VERBOSE is True
