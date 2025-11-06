"""
BERDL Agent implementation using LangChain.

This module provides the main agent class and factory functions for creating
intelligent assistants that can interact with the BERDL data lake.
"""

import logging
from typing import Any, Iterator

from langchain.agents import AgentExecutor, create_react_agent, create_openai_tools_agent
from langchain.memory import ConversationBufferMemory
from langchain_anthropic import ChatAnthropic
from langchain_community.chat_models import ChatOllama
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate, MessagesPlaceholder
from langchain_openai import ChatOpenAI

from berdl_notebook_utils import get_settings
from berdl_notebook_utils.agent.prompts import get_system_prompt
from berdl_notebook_utils.agent.settings import get_agent_settings
from berdl_notebook_utils.agent.tools import get_all_tools
from berdl_notebook_utils.agent.mcp_tools import get_mcp_tools

logger = logging.getLogger(__name__)


# ReAct prompt template for LangChain
REACT_PROMPT_TEMPLATE = """
{system_prompt}

TOOLS:
------

You have access to the following tools:

{tools}

To use a tool, please use the following format:

```
Thought: Do I need to use a tool? Yes
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
```

When you have a response to say to the Human, or if you do not need to use a tool, you MUST use the format:

```
Thought: Do I need to use a tool? No
Final Answer: [your response here]
```

Begin!

Previous conversation history:
{chat_history}

New input: {input}
{agent_scratchpad}
"""


class BERDLAgent:
    """
    Intelligent agent for interacting with the BERDL data lake.

    This agent uses LangChain and configurable LLM backends (Anthropic, OpenAI, Ollama)
    to provide natural language access to data lake operations.

    Example:
        >>> agent = BERDLAgent()
        >>> result = agent.run("What databases do I have?")
        >>> print(result)
    """

    def __init__(
        self,
        model: str | None = None,
        model_provider: str | None = None,
        temperature: float | None = None,
        enable_sql_execution: bool | None = None,
        verbose: bool | None = True,
        enable_memory: bool | None = None,
        use_mcp_tools: bool = True,
        mcp_server_url: str | None = None,
    ):
        """
        Initialize the BERDL agent.

        Args:
            model: Model name (e.g., "claude-3-5-sonnet-20241022", "gpt-4o")
            model_provider: Provider name ("anthropic", "openai", "ollama")
            temperature: LLM temperature (0.0 = deterministic, 2.0 = creative)
            enable_sql_execution: Enable SQL query execution tools
            verbose: Enable verbose logging of agent reasoning
            enable_memory: Enable conversation memory
            use_mcp_tools: Use native MCP tools instead of manual wrappers (default: False)
            mcp_server_url: MCP server URL (default: http://localhost:8005/apis/mcp/mcp)
        """
        # Load settings with overrides
        self.settings = get_agent_settings()
        self.berdl_settings = get_settings()

        # Apply overrides
        self.model_provider = model_provider or self.settings.AGENT_MODEL_PROVIDER
        self.model = model or self.settings.AGENT_MODEL_NAME
        self.temperature = temperature if temperature is not None else self.settings.AGENT_TEMPERATURE
        self.enable_sql_execution = (
            enable_sql_execution if enable_sql_execution is not None else self.settings.AGENT_ENABLE_SQL_EXECUTION
        )
        self.verbose = verbose if verbose is not None else self.settings.AGENT_VERBOSE
        self.enable_memory = enable_memory if enable_memory is not None else self.settings.AGENT_ENABLE_MEMORY
        self.use_mcp_tools = use_mcp_tools

        # Initialize LLM
        self.llm = self._create_llm()

        # Initialize tools - use MCP or manual wrappers
        if self.use_mcp_tools:
            logger.info("Loading tools from MCP server")
            self.tools = get_mcp_tools(server_url=mcp_server_url)
            logger.info(f"Loaded {len(self.tools)} tools from MCP server")
        else:
            # Use manual tool wrappers (default behavior)
            self.tools = get_all_tools(
                enable_sql_execution=self.enable_sql_execution,
            )

        # Initialize memory
        # For OpenAI tools agent, we need messages format; for ReAct, we use string format
        self.memory = None
        if self.enable_memory:
            return_messages = self.model_provider == "openai"  # OpenAI needs messages
            self.memory = ConversationBufferMemory(
                memory_key="chat_history",
                input_key="input",
                output_key="output",
                return_messages=return_messages,
            )

        # Create prompt and agent based on provider
        system_prompt = get_system_prompt(
            username=self.berdl_settings.USER,
        )

        # Use native function calling for OpenAI, ReAct for others
        if self.model_provider == "openai":
            # OpenAI supports native function calling - use it for better tool handling
            self.prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", system_prompt),
                    MessagesPlaceholder(variable_name="chat_history", optional=True),
                    ("human", "{input}"),
                    MessagesPlaceholder(variable_name="agent_scratchpad"),
                ]
            )

            self.agent = create_openai_tools_agent(
                llm=self.llm,
                tools=self.tools,
                prompt=self.prompt,
            )
        else:
            # Use ReAct for Anthropic, Ollama, and other providers
            self.prompt = PromptTemplate.from_template(REACT_PROMPT_TEMPLATE).partial(system_prompt=system_prompt)

            self.agent = create_react_agent(
                llm=self.llm,
                tools=self.tools,
                prompt=self.prompt,
            )

        # Create agent executor
        self.executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=self.verbose,
            memory=self.memory,
            handle_parsing_errors=True,
            max_iterations=self.settings.AGENT_MAX_ITERATIONS,
            max_execution_time=self.settings.AGENT_MAX_EXECUTION_TIME,
        )

        logger.info(f"BERDL Agent initialized: provider={self.model_provider}, model={self.model}")

    def _create_llm(self) -> Any:
        """Create LLM based on configured provider."""
        if self.model_provider == "anthropic":
            api_key = self.settings.AGENT_ANTHROPIC_API_KEY
            if not api_key:
                raise ValueError("ANTHROPIC_API_KEY not set. Please set it in environment variables or BERDLSettings.")
            return ChatAnthropic(
                model=self.model,
                temperature=self.temperature,
                api_key=api_key,
            )

        elif self.model_provider == "openai":
            api_key = self.settings.AGENT_OPENAI_API_KEY
            if not api_key:
                raise ValueError("OPENAI_API_KEY not set. Please set it in environment variables or BERDLSettings.")
            return ChatOpenAI(
                model=self.model,
                temperature=self.temperature,
                api_key=api_key,
            )

        elif self.model_provider == "ollama":
            return ChatOllama(
                model=self.model,
                temperature=self.temperature,
                base_url=self.settings.AGENT_OLLAMA_BASE_URL,
            )

        else:
            raise ValueError(
                f"Unknown model provider: {self.model_provider}. Must be 'anthropic', 'openai', or 'ollama'."
            )

    def run(self, query: str) -> str:
        """
        Run a query against the agent and return the result.

        Args:
            query: Natural language query

        Returns:
            Agent's response as a string

        Example:
            >>> agent = BERDLAgent()
            >>> result = agent.run("What tables are in my research database?")
        """
        try:
            result = self.executor.invoke({"input": query})
            return result.get("output", "No response generated.")
        except Exception as e:
            logger.error(f"Error running agent: {e}")
            return f"Error: {e}"

    def chat(self, message: str) -> str:
        """
        Chat with the agent (alias for run() for better UX).

        Args:
            message: User message

        Returns:
            Agent's response
        """
        return self.run(message)

    def stream(self, query: str) -> Iterator[str]:
        """
        Stream the agent's response token by token.

        Args:
            query: Natural language query

        Yields:
            Response chunks as they become available

        Example:
            >>> agent = BERDLAgent()
            >>> for chunk in agent.stream("Analyze my data"):
            ...     print(chunk, end="", flush=True)
        """
        # Note: LangChain's AgentExecutor doesn't support native streaming for all backends
        # This is a simplified version that yields the full response
        # For true streaming, we'd need to use astream() and async
        try:
            result = self.executor.invoke({"input": query})
            output = result.get("output", "No response generated.")
            # Simulate streaming by yielding the full output
            # (true streaming would require async implementation)
            yield output
        except Exception as e:
            logger.error(f"Error streaming agent response: {e}")
            yield f"Error: {e}"

    def clear_memory(self):
        """Clear the conversation memory."""
        if self.memory:
            self.memory.clear()
            logger.info("Conversation memory cleared.")

    def get_conversation_history(self) -> list[dict]:
        """
        Get the conversation history.

        Returns:
            List of conversation turns (if memory is enabled)
        """
        if not self.memory:
            return []

        # Return the chat history as a list of dicts
        history = self.memory.load_memory_variables({})
        return history.get("chat_history", [])

    def __repr__(self) -> str:
        """String representation of the agent."""
        return f"BERDLAgent(provider={self.model_provider}, model={self.model}, tools={len(self.tools)})"


# =============================================================================
# Factory Functions
# =============================================================================


def create_berdl_agent(
    model: str | None = None,
    model_provider: str | None = None,
    temperature: float | None = 0,
    enable_sql_execution: bool | None = True,
    verbose: bool | None = True,
    enable_memory: bool | None = True,
    use_mcp_tools: bool = True,
    mcp_server_url: str | None = None,
) -> BERDLAgent:
    """
    Create a BERDL agent with specified configuration.

    This is the main entry point for creating agents. It provides a simple API
    with sensible defaults.

    Args:
        model: Model name (default: from settings, usually "claude-3-5-sonnet-20241022")
        model_provider: "anthropic", "openai", or "ollama" (default: from settings)
        temperature: LLM temperature 0.0-2.0 (default: 0.0 for deterministic)
        enable_sql_execution: Enable SQL query execution tools (default: from settings)
        verbose: Show agent reasoning steps (default: True)
        enable_memory: Enable conversation memory (default: from settings)
        use_mcp_tools: Use native MCP tools from running server (default: True)
        mcp_server_url: MCP server URL (default: http://localhost:8005/apis/mcp/mcp)

    Returns:
        Configured BERDLAgent instance

    Example:
        >>> # Use default settings (Anthropic Claude)
        >>> agent = create_berdl_agent()
        >>> result = agent.run("What databases exist?")

        >>> # Use GPT-4o with verbose output
        >>> agent = create_berdl_agent(model="gpt-4o", model_provider="openai", verbose=True)

        >>> # Disable SQL execution (discovery and inspection only)
        >>> agent = create_berdl_agent(enable_sql_execution=False)
    """
    return BERDLAgent(
        model=model,
        model_provider=model_provider,
        temperature=temperature,
        enable_sql_execution=enable_sql_execution,
        verbose=verbose,
        enable_memory=enable_memory,
        use_mcp_tools=use_mcp_tools,
        mcp_server_url=mcp_server_url,
    )
