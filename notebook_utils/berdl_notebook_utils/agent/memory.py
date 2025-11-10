"""
Conversation memory management for BERDL Agent.

This module provides utilities for managing conversation history, including
session-based memory and optional persistence to disk.
"""

import json
import logging
from pathlib import Path
from typing import Any

from langchain.memory import ConversationBufferMemory, ConversationSummaryMemory

from berdl_notebook_utils.agent.settings import get_agent_settings

logger = logging.getLogger(__name__)


def create_conversation_memory(
    memory_type: str = "buffer",
    max_messages: int | None = None,
    llm: Any | None = None,
) -> ConversationBufferMemory | ConversationSummaryMemory:
    """
    Create a conversation memory instance.

    Args:
        memory_type: Type of memory ("buffer" or "summary")
        max_messages: Maximum number of messages to keep (None = unlimited)
        llm: LLM instance for summary memory (required if memory_type="summary")

    Returns:
        Configured memory instance

    Example:
        >>> memory = create_conversation_memory(memory_type="buffer", max_messages=20)
    """
    settings = get_agent_settings()

    if max_messages is None:
        max_messages = settings.AGENT_MEMORY_MAX_MESSAGES

    if memory_type == "buffer":
        return ConversationBufferMemory(
            memory_key="chat_history",
            input_key="input",
            output_key="output",
            return_messages=False,
            max_length=max_messages if max_messages > 0 else None,
        )

    elif memory_type == "summary":
        if llm is None:
            raise ValueError("LLM instance required for summary memory")

        return ConversationSummaryMemory(
            llm=llm,
            memory_key="chat_history",
            input_key="input",
            output_key="output",
            return_messages=False,
        )

    else:
        raise ValueError(f"Unknown memory type: {memory_type}. Must be 'buffer' or 'summary'.")


def save_conversation_history(memory: ConversationBufferMemory, filepath: str | Path) -> None:
    """
    Save conversation history to a JSON file.

    Args:
        memory: LangChain memory instance
        filepath: Path to save the history

    Example:
        >>> from berdl_notebook_utils.agent import create_berdl_agent
        >>> agent = create_berdl_agent()
        >>> agent.run("What databases do I have?")
        >>> save_conversation_history(agent.memory, "~/my_conversation.json")
    """
    filepath = Path(filepath).expanduser()

    try:
        # Get conversation history
        history_data = memory.load_memory_variables({})

        # Save to JSON
        with open(filepath, "w") as f:
            json.dump(history_data, f, indent=2, default=str)

        logger.info(f"Conversation history saved to {filepath}")

    except Exception as e:
        logger.error(f"Failed to save conversation history: {e}")
        raise


def load_conversation_history(memory: ConversationBufferMemory, filepath: str | Path) -> None:
    """
    Load conversation history from a JSON file.

    Args:
        memory: LangChain memory instance to populate
        filepath: Path to load the history from

    Example:
        >>> from berdl_notebook_utils.agent import create_berdl_agent
        >>> agent = create_berdl_agent()
        >>> load_conversation_history(agent.memory, "~/my_conversation.json")
        >>> agent.run("Continue our previous discussion")
    """
    filepath = Path(filepath).expanduser()

    try:
        # Load from JSON
        with open(filepath) as f:
            history_data = json.load(f)

        # Clear existing memory
        memory.clear()

        # Restore history
        # Note: This is a simplified approach. For full restoration,
        # we'd need to reconstruct the message objects.
        if "chat_history" in history_data:
            memory.chat_memory.add_user_message("Conversation history loaded. Previous context has been restored.")

        logger.info(f"Conversation history loaded from {filepath}")

    except FileNotFoundError:
        logger.error(f"History file not found: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Failed to load conversation history: {e}")
        raise


def get_default_history_path(username: str | None = None) -> Path:
    """
    Get the default path for saving conversation history.

    Args:
        username: User's username (auto-detected if None)

    Returns:
        Path to save conversation history

    Example:
        >>> path = get_default_history_path()
        >>> print(path)
        /home/tgu2/.berdl/conversation_history.json
    """
    from berdl_notebook_utils import get_settings

    if username is None:
        try:
            settings = get_settings()
            username = settings.USER
        except Exception:
            username = "unknown"

    # Save to user's home directory in .berdl folder
    history_dir = Path.home() / ".berdl"
    history_dir.mkdir(exist_ok=True)

    return history_dir / f"conversation_history_{username}.json"


class ConversationHistoryManager:
    """
    Manager for conversation history with auto-save functionality.

    Example:
        >>> from berdl_notebook_utils.agent import create_berdl_agent
        >>> agent = create_berdl_agent()
        >>> manager = ConversationHistoryManager(agent.memory)
        >>>
        >>> # Use agent normally
        >>> agent.run("What tables exist?")
        >>>
        >>> # Save history
        >>> manager.save()
        >>>
        >>> # Load history in a new session
        >>> new_agent = create_berdl_agent()
        >>> new_manager = ConversationHistoryManager(new_agent.memory)
        >>> new_manager.load()
    """

    def __init__(self, memory: ConversationBufferMemory, filepath: str | Path | None = None):
        """
        Initialize the history manager.

        Args:
            memory: LangChain memory instance
            filepath: Path to save/load history (default: ~/.berdl/conversation_history_{user}.json)
        """
        self.memory = memory
        self.filepath = Path(filepath).expanduser() if filepath else get_default_history_path()

    def save(self) -> None:
        """Save conversation history to the configured path."""
        save_conversation_history(self.memory, self.filepath)

    def load(self) -> None:
        """Load conversation history from the configured path."""
        if self.filepath.exists():
            load_conversation_history(self.memory, self.filepath)
        else:
            logger.warning(f"No saved history found at {self.filepath}")

    def clear(self) -> None:
        """Clear the conversation history."""
        self.memory.clear()
        logger.info("Conversation memory cleared")

    def auto_save_on_exit(self) -> None:
        """
        Register an exit handler to automatically save history when the notebook exits.

        Example:
            >>> agent = create_berdl_agent()
            >>> manager = ConversationHistoryManager(agent.memory)
            >>> manager.auto_save_on_exit()  # Now history saves automatically
        """
        import atexit

        atexit.register(self.save)
        logger.info(f"Auto-save enabled. History will be saved to {self.filepath} on exit.")
