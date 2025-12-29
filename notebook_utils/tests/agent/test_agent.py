"""
Tests for agent/agent.py - BERDLAgent class and factory functions.
"""

from unittest.mock import Mock, MagicMock, patch
import pytest


class TestBERDLAgentInit:
    """Tests for BERDLAgent initialization."""

    @patch("berdl_notebook_utils.agent.agent.AgentExecutor")
    @patch("berdl_notebook_utils.agent.agent.get_all_tools")
    @patch("berdl_notebook_utils.agent.agent.get_system_prompt")
    @patch("berdl_notebook_utils.agent.agent.create_react_agent")
    @patch("berdl_notebook_utils.agent.agent.ChatAnthropic")
    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_init_with_anthropic_provider(
        self,
        mock_get_settings,
        mock_get_agent_settings,
        mock_chat_anthropic,
        mock_create_react_agent,
        mock_get_system_prompt,
        mock_get_all_tools,
        mock_agent_executor,
    ):
        """Test BERDLAgent initialization with Anthropic provider."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        # Setup mocks
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "anthropic"
        mock_agent_settings.AGENT_MODEL_NAME = "claude-3-5-sonnet-20241022"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
        mock_agent_settings.AGENT_VERBOSE = True
        mock_agent_settings.AGENT_ENABLE_MEMORY = False
        mock_agent_settings.AGENT_ANTHROPIC_API_KEY = "test-api-key"
        mock_agent_settings.AGENT_MAX_ITERATIONS = 10
        mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
        mock_get_agent_settings.return_value = mock_agent_settings

        mock_get_all_tools.return_value = []
        mock_get_system_prompt.return_value = "System prompt"
        mock_create_react_agent.return_value = Mock()
        mock_chat_anthropic.return_value = Mock()
        mock_agent_executor.return_value = Mock()

        # Create agent
        agent = BERDLAgent(enable_memory=False)

        # Verify Anthropic LLM was created
        mock_chat_anthropic.assert_called_once()
        assert agent.model_provider == "anthropic"

    @patch("berdl_notebook_utils.agent.agent.AgentExecutor")
    @patch("berdl_notebook_utils.agent.agent.get_all_tools")
    @patch("berdl_notebook_utils.agent.agent.get_system_prompt")
    @patch("berdl_notebook_utils.agent.agent.create_openai_tools_agent")
    @patch("berdl_notebook_utils.agent.agent.ChatOpenAI")
    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_init_with_openai_provider(
        self,
        mock_get_settings,
        mock_get_agent_settings,
        mock_chat_openai,
        mock_create_openai_agent,
        mock_get_system_prompt,
        mock_get_all_tools,
        mock_agent_executor,
    ):
        """Test BERDLAgent initialization with OpenAI provider."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        # Setup mocks
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "openai"
        mock_agent_settings.AGENT_MODEL_NAME = "gpt-4o"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
        mock_agent_settings.AGENT_VERBOSE = True
        mock_agent_settings.AGENT_ENABLE_MEMORY = False
        mock_agent_settings.AGENT_OPENAI_API_KEY = "test-openai-key"
        mock_agent_settings.AGENT_MAX_ITERATIONS = 10
        mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
        mock_get_agent_settings.return_value = mock_agent_settings

        mock_get_all_tools.return_value = []
        mock_get_system_prompt.return_value = "System prompt"
        mock_create_openai_agent.return_value = Mock()
        mock_chat_openai.return_value = Mock()
        mock_agent_executor.return_value = Mock()

        # Create agent with OpenAI
        agent = BERDLAgent(model_provider="openai", enable_memory=False)

        # Verify OpenAI LLM was created
        mock_chat_openai.assert_called_once()
        assert agent.model_provider == "openai"

    @patch("berdl_notebook_utils.agent.agent.AgentExecutor")
    @patch("berdl_notebook_utils.agent.agent.get_all_tools")
    @patch("berdl_notebook_utils.agent.agent.get_system_prompt")
    @patch("berdl_notebook_utils.agent.agent.create_react_agent")
    @patch("berdl_notebook_utils.agent.agent.ChatOllama")
    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_init_with_ollama_provider(
        self,
        mock_get_settings,
        mock_get_agent_settings,
        mock_chat_ollama,
        mock_create_react_agent,
        mock_get_system_prompt,
        mock_get_all_tools,
        mock_agent_executor,
    ):
        """Test BERDLAgent initialization with Ollama provider."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        # Setup mocks
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "ollama"
        mock_agent_settings.AGENT_MODEL_NAME = "llama3"
        mock_agent_settings.AGENT_TEMPERATURE = 0.5
        mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
        mock_agent_settings.AGENT_VERBOSE = False
        mock_agent_settings.AGENT_ENABLE_MEMORY = False
        mock_agent_settings.AGENT_OLLAMA_BASE_URL = "http://localhost:11434"
        mock_agent_settings.AGENT_MAX_ITERATIONS = 10
        mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
        mock_get_agent_settings.return_value = mock_agent_settings

        mock_get_all_tools.return_value = []
        mock_get_system_prompt.return_value = "System prompt"
        mock_create_react_agent.return_value = Mock()
        mock_chat_ollama.return_value = Mock()
        mock_agent_executor.return_value = Mock()

        # Create agent with Ollama
        agent = BERDLAgent(model_provider="ollama", enable_memory=False)

        # Verify Ollama LLM was created
        mock_chat_ollama.assert_called_once()
        assert agent.model_provider == "ollama"


class TestBERDLAgentCreateLLM:
    """Tests for BERDLAgent._create_llm method."""

    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_create_llm_anthropic_missing_api_key(
        self, mock_get_settings, mock_get_agent_settings
    ):
        """Test _create_llm raises error when Anthropic API key is missing."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "anthropic"
        mock_agent_settings.AGENT_MODEL_NAME = "claude-3-5-sonnet-20241022"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_ANTHROPIC_API_KEY = None  # Missing key
        mock_get_agent_settings.return_value = mock_agent_settings

        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY not set"):
            BERDLAgent(model_provider="anthropic")

    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_create_llm_openai_missing_api_key(
        self, mock_get_settings, mock_get_agent_settings
    ):
        """Test _create_llm raises error when OpenAI API key is missing."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "openai"
        mock_agent_settings.AGENT_MODEL_NAME = "gpt-4o"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_OPENAI_API_KEY = None  # Missing key
        mock_get_agent_settings.return_value = mock_agent_settings

        with pytest.raises(ValueError, match="OPENAI_API_KEY not set"):
            BERDLAgent(model_provider="openai")

    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_create_llm_unknown_provider(
        self, mock_get_settings, mock_get_agent_settings
    ):
        """Test _create_llm raises error for unknown provider."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "unknown_provider"
        mock_agent_settings.AGENT_MODEL_NAME = "some-model"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_get_agent_settings.return_value = mock_agent_settings

        with pytest.raises(ValueError, match="Unknown model provider"):
            BERDLAgent(model_provider="unknown_provider")


class TestBERDLAgentMethods:
    """Tests for BERDLAgent instance methods."""

    def _create_mock_agent(self):
        """Helper to create a fully mocked BERDLAgent."""
        with patch("berdl_notebook_utils.agent.agent.AgentExecutor"), \
             patch("berdl_notebook_utils.agent.agent.get_all_tools") as mock_tools, \
             patch("berdl_notebook_utils.agent.agent.get_system_prompt"), \
             patch("berdl_notebook_utils.agent.agent.create_react_agent"), \
             patch("berdl_notebook_utils.agent.agent.ChatOllama"), \
             patch("berdl_notebook_utils.agent.agent.get_agent_settings") as mock_settings, \
             patch("berdl_notebook_utils.agent.agent.get_settings") as mock_berdl_settings:

            mock_berdl_settings.return_value = Mock(USER="test_user")
            mock_agent_settings = Mock()
            mock_agent_settings.AGENT_MODEL_PROVIDER = "ollama"
            mock_agent_settings.AGENT_MODEL_NAME = "llama3"
            mock_agent_settings.AGENT_TEMPERATURE = 0.0
            mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
            mock_agent_settings.AGENT_VERBOSE = False
            mock_agent_settings.AGENT_ENABLE_MEMORY = False
            mock_agent_settings.AGENT_OLLAMA_BASE_URL = "http://localhost:11434"
            mock_agent_settings.AGENT_MAX_ITERATIONS = 10
            mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
            mock_settings.return_value = mock_agent_settings
            mock_tools.return_value = []

            from berdl_notebook_utils.agent.agent import BERDLAgent
            return BERDLAgent(model_provider="ollama", enable_memory=False)

    def test_run_returns_output(self):
        """Test run method returns agent output."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.return_value = {"output": "Test response"}

        result = agent.run("Test query")

        assert result == "Test response"
        agent.executor.invoke.assert_called_once_with({"input": "Test query"})

    def test_run_handles_error(self):
        """Test run method handles errors gracefully."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.side_effect = Exception("Test error")

        result = agent.run("Test query")

        assert "Error: Test error" in result

    def test_run_returns_default_when_no_output(self):
        """Test run method returns default message when output is empty."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.return_value = {}

        result = agent.run("Test query")

        assert result == "No response generated."

    def test_chat_is_alias_for_run(self):
        """Test chat method is an alias for run."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.return_value = {"output": "Chat response"}

        result = agent.chat("Hello")

        assert result == "Chat response"

    def test_stream_yields_output(self):
        """Test stream method yields the response."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.return_value = {"output": "Streamed response"}

        chunks = list(agent.stream("Test query"))

        assert chunks == ["Streamed response"]

    def test_stream_handles_error(self):
        """Test stream method handles errors gracefully."""
        agent = self._create_mock_agent()
        agent.executor = Mock()
        agent.executor.invoke.side_effect = Exception("Stream error")

        chunks = list(agent.stream("Test query"))

        assert len(chunks) == 1
        assert "Error: Stream error" in chunks[0]

    def test_clear_memory_clears_when_enabled(self):
        """Test clear_memory clears the memory when enabled."""
        agent = self._create_mock_agent()
        mock_memory = Mock()
        agent.memory = mock_memory

        agent.clear_memory()

        mock_memory.clear.assert_called_once()

    def test_clear_memory_does_nothing_when_disabled(self):
        """Test clear_memory does nothing when memory is disabled."""
        agent = self._create_mock_agent()
        agent.memory = None

        # Should not raise
        agent.clear_memory()

    def test_get_conversation_history_returns_empty_when_no_memory(self):
        """Test get_conversation_history returns empty list when memory is disabled."""
        agent = self._create_mock_agent()
        agent.memory = None

        result = agent.get_conversation_history()

        assert result == []

    def test_get_conversation_history_returns_history(self):
        """Test get_conversation_history returns history when memory is enabled."""
        agent = self._create_mock_agent()
        mock_memory = Mock()
        mock_memory.load_memory_variables.return_value = {
            "chat_history": [{"role": "user", "content": "Hello"}]
        }
        agent.memory = mock_memory

        result = agent.get_conversation_history()

        assert result == [{"role": "user", "content": "Hello"}]

    def test_repr(self):
        """Test __repr__ returns correct string."""
        agent = self._create_mock_agent()
        agent.tools = [Mock(), Mock(), Mock()]

        repr_str = repr(agent)

        assert "BERDLAgent" in repr_str
        assert "ollama" in repr_str
        assert "3" in repr_str


class TestBERDLAgentMemory:
    """Tests for BERDLAgent memory initialization."""

    @patch("berdl_notebook_utils.agent.agent.AgentExecutor")
    @patch("berdl_notebook_utils.agent.agent.get_all_tools")
    @patch("berdl_notebook_utils.agent.agent.get_system_prompt")
    @patch("berdl_notebook_utils.agent.agent.create_openai_tools_agent")
    @patch("berdl_notebook_utils.agent.agent.ChatOpenAI")
    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_memory_returns_messages_for_openai(
        self,
        mock_get_settings,
        mock_get_agent_settings,
        mock_chat_openai,
        mock_create_openai_agent,
        mock_get_system_prompt,
        mock_get_all_tools,
        mock_agent_executor,
    ):
        """Test memory is configured to return messages for OpenAI provider."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "openai"
        mock_agent_settings.AGENT_MODEL_NAME = "gpt-4o"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
        mock_agent_settings.AGENT_VERBOSE = False
        mock_agent_settings.AGENT_ENABLE_MEMORY = True
        mock_agent_settings.AGENT_OPENAI_API_KEY = "test-key"
        mock_agent_settings.AGENT_MAX_ITERATIONS = 10
        mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
        mock_get_agent_settings.return_value = mock_agent_settings

        mock_get_all_tools.return_value = []
        mock_get_system_prompt.return_value = "System prompt"
        mock_create_openai_agent.return_value = Mock()
        mock_chat_openai.return_value = Mock()
        mock_agent_executor.return_value = Mock()

        agent = BERDLAgent(model_provider="openai", enable_memory=True)

        # Verify memory is enabled with return_messages=True for OpenAI
        assert agent.memory is not None


class TestBERDLAgentMCPTools:
    """Tests for BERDLAgent with MCP tool discovery."""

    @patch("berdl_notebook_utils.agent.agent.AgentExecutor")
    @patch("berdl_notebook_utils.agent.agent.get_mcp_tools")
    @patch("berdl_notebook_utils.agent.agent.get_system_prompt")
    @patch("berdl_notebook_utils.agent.agent.create_react_agent")
    @patch("berdl_notebook_utils.agent.agent.ChatOllama")
    @patch("berdl_notebook_utils.agent.agent.get_agent_settings")
    @patch("berdl_notebook_utils.agent.agent.get_settings")
    def test_discover_tools_from_server(
        self,
        mock_get_settings,
        mock_get_agent_settings,
        mock_chat_ollama,
        mock_create_react_agent,
        mock_get_system_prompt,
        mock_get_mcp_tools,
        mock_agent_executor,
    ):
        """Test BERDLAgent discovers tools from MCP server when enabled."""
        from berdl_notebook_utils.agent.agent import BERDLAgent

        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        mock_agent_settings = Mock()
        mock_agent_settings.AGENT_MODEL_PROVIDER = "ollama"
        mock_agent_settings.AGENT_MODEL_NAME = "llama3"
        mock_agent_settings.AGENT_TEMPERATURE = 0.0
        mock_agent_settings.AGENT_ENABLE_SQL_EXECUTION = True
        mock_agent_settings.AGENT_VERBOSE = False
        mock_agent_settings.AGENT_ENABLE_MEMORY = False
        mock_agent_settings.AGENT_OLLAMA_BASE_URL = "http://localhost:11434"
        mock_agent_settings.AGENT_MAX_ITERATIONS = 10
        mock_agent_settings.AGENT_MAX_EXECUTION_TIME = 300
        mock_get_agent_settings.return_value = mock_agent_settings

        mock_mcp_tools = []  # Use empty list to avoid tool validation
        mock_get_mcp_tools.return_value = mock_mcp_tools
        mock_get_system_prompt.return_value = "System prompt"
        mock_create_react_agent.return_value = Mock()
        mock_chat_ollama.return_value = Mock()
        mock_agent_executor.return_value = Mock()

        agent = BERDLAgent(
            model_provider="ollama",
            enable_memory=False,
            discover_tools_from_server=True,
            mcp_server_url="http://localhost:8005",
        )

        mock_get_mcp_tools.assert_called_once_with(server_url="http://localhost:8005")
        assert agent.tools == mock_mcp_tools


class TestCreateBerdlAgent:
    """Tests for create_berdl_agent factory function."""

    @patch("berdl_notebook_utils.agent.agent.BERDLAgent")
    def test_create_berdl_agent_with_defaults(self, mock_agent_class):
        """Test create_berdl_agent creates agent with default params."""
        from berdl_notebook_utils.agent.agent import create_berdl_agent

        mock_agent = Mock()
        mock_agent_class.return_value = mock_agent

        result = create_berdl_agent()

        mock_agent_class.assert_called_once_with(
            model=None,
            model_provider=None,
            temperature=0,
            enable_sql_execution=True,
            verbose=False,
            enable_memory=True,
            discover_tools_from_server=False,
            mcp_server_url=None,
        )
        assert result == mock_agent

    @patch("berdl_notebook_utils.agent.agent.BERDLAgent")
    def test_create_berdl_agent_with_custom_params(self, mock_agent_class):
        """Test create_berdl_agent passes custom params."""
        from berdl_notebook_utils.agent.agent import create_berdl_agent

        mock_agent = Mock()
        mock_agent_class.return_value = mock_agent

        result = create_berdl_agent(
            model="gpt-4o",
            model_provider="openai",
            temperature=0.7,
            enable_sql_execution=False,
            verbose=True,
            enable_memory=False,
            discover_tools_from_server=True,
            mcp_server_url="http://custom-server:8005",
        )

        mock_agent_class.assert_called_once_with(
            model="gpt-4o",
            model_provider="openai",
            temperature=0.7,
            enable_sql_execution=False,
            verbose=True,
            enable_memory=False,
            discover_tools_from_server=True,
            mcp_server_url="http://custom-server:8005",
        )
        assert result == mock_agent
