"""
Tests for agent/memory.py - Conversation memory management.
"""

import json
import logging
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from berdl_notebook_utils.agent.memory import (
    create_conversation_memory,
    save_conversation_history,
    load_conversation_history,
    get_default_history_path,
    ConversationHistoryManager,
)


class TestCreateConversationMemory:
    """Tests for create_conversation_memory function."""

    @patch("berdl_notebook_utils.agent.memory.get_agent_settings")
    def test_create_buffer_memory(self, mock_get_agent_settings):
        """Test creating buffer memory type."""
        mock_settings = Mock()
        mock_settings.AGENT_MEMORY_MAX_MESSAGES = 20
        mock_get_agent_settings.return_value = mock_settings

        memory = create_conversation_memory(memory_type="buffer")

        assert memory is not None
        assert memory.memory_key == "chat_history"
        assert memory.input_key == "input"
        assert memory.output_key == "output"

    @patch("berdl_notebook_utils.agent.memory.get_agent_settings")
    def test_create_buffer_memory_with_custom_max_messages(self, mock_get_agent_settings):
        """Test creating buffer memory with custom max_messages."""
        mock_settings = Mock()
        mock_settings.AGENT_MEMORY_MAX_MESSAGES = 20
        mock_get_agent_settings.return_value = mock_settings

        memory = create_conversation_memory(memory_type="buffer", max_messages=50)

        assert memory is not None

    @patch("berdl_notebook_utils.agent.memory.ConversationSummaryMemory")
    @patch("berdl_notebook_utils.agent.memory.get_agent_settings")
    def test_create_summary_memory_with_llm(self, mock_get_agent_settings, mock_summary_memory):
        """Test creating summary memory with LLM."""
        mock_settings = Mock()
        mock_settings.AGENT_MEMORY_MAX_MESSAGES = 20
        mock_get_agent_settings.return_value = mock_settings

        mock_llm = Mock()
        mock_summary_memory.return_value = Mock()

        create_conversation_memory(memory_type="summary", llm=mock_llm)

        mock_summary_memory.assert_called_once()
        call_kwargs = mock_summary_memory.call_args[1]
        assert call_kwargs["llm"] == mock_llm
        assert call_kwargs["memory_key"] == "chat_history"

    @patch("berdl_notebook_utils.agent.memory.get_agent_settings")
    def test_create_summary_memory_without_llm_raises(self, mock_get_agent_settings):
        """Test creating summary memory without LLM raises error."""
        mock_settings = Mock()
        mock_settings.AGENT_MEMORY_MAX_MESSAGES = 20
        mock_get_agent_settings.return_value = mock_settings

        with pytest.raises(ValueError, match="LLM instance required"):
            create_conversation_memory(memory_type="summary")

    @patch("berdl_notebook_utils.agent.memory.get_agent_settings")
    def test_unknown_memory_type_raises(self, mock_get_agent_settings):
        """Test unknown memory type raises error."""
        mock_settings = Mock()
        mock_settings.AGENT_MEMORY_MAX_MESSAGES = 20
        mock_get_agent_settings.return_value = mock_settings

        with pytest.raises(ValueError, match="Unknown memory type"):
            create_conversation_memory(memory_type="unknown")


class TestSaveConversationHistory:
    """Tests for save_conversation_history function."""

    def test_save_conversation_history_to_file(self, tmp_path):
        """Test saving conversation history to a JSON file."""
        mock_memory = Mock()
        mock_memory.load_memory_variables.return_value = {"chat_history": "User: Hello\nAssistant: Hi there!"}

        filepath = tmp_path / "test_history.json"
        save_conversation_history(mock_memory, filepath)

        assert filepath.exists()
        with open(filepath) as f:
            saved_data = json.load(f)
        assert "chat_history" in saved_data
        assert saved_data["chat_history"] == "User: Hello\nAssistant: Hi there!"

    def test_save_conversation_history_creates_parent_dirs(self, tmp_path):
        """Test saving creates parent directories if needed."""
        mock_memory = Mock()
        mock_memory.load_memory_variables.return_value = {"chat_history": "test"}

        filepath = tmp_path / "subdir" / "test_history.json"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        save_conversation_history(mock_memory, filepath)

        assert filepath.exists()

    def test_save_conversation_history_handles_error(self, tmp_path):
        """Test saving raises error on failure."""
        mock_memory = Mock()
        mock_memory.load_memory_variables.side_effect = Exception("Memory error")

        filepath = tmp_path / "test_history.json"

        with pytest.raises(Exception, match="Memory error"):
            save_conversation_history(mock_memory, filepath)


class TestLoadConversationHistory:
    """Tests for load_conversation_history function."""

    def test_load_conversation_history_from_file(self, tmp_path):
        """Test loading conversation history from a JSON file."""
        # Create a history file
        filepath = tmp_path / "test_history.json"
        with open(filepath, "w") as f:
            json.dump({"chat_history": "Previous conversation"}, f)

        mock_memory = Mock()
        mock_memory.chat_memory = Mock()

        load_conversation_history(mock_memory, filepath)

        mock_memory.clear.assert_called_once()
        mock_memory.chat_memory.add_user_message.assert_called_once()

    def test_load_conversation_history_file_not_found(self, tmp_path):
        """Test loading raises error when file not found."""
        filepath = tmp_path / "nonexistent.json"
        mock_memory = Mock()

        with pytest.raises(FileNotFoundError):
            load_conversation_history(mock_memory, filepath)

    def test_load_conversation_history_invalid_json(self, tmp_path):
        """Test loading raises error with invalid JSON."""
        filepath = tmp_path / "invalid.json"
        with open(filepath, "w") as f:
            f.write("not valid json")

        mock_memory = Mock()

        with pytest.raises(Exception):
            load_conversation_history(mock_memory, filepath)


class TestGetDefaultHistoryPath:
    """Tests for get_default_history_path function."""

    @patch("berdl_notebook_utils.get_settings")
    def test_get_default_history_path_with_settings(self, mock_get_settings):
        """Test getting default history path from settings."""
        mock_settings = Mock()
        mock_settings.USER = "test_user"
        mock_get_settings.return_value = mock_settings

        path = get_default_history_path()

        assert path.name == "conversation_history_test_user.json"
        assert ".berdl" in str(path)

    def test_get_default_history_path_with_username(self):
        """Test getting default history path with explicit username."""
        path = get_default_history_path(username="custom_user")

        assert path.name == "conversation_history_custom_user.json"
        assert ".berdl" in str(path)

    @patch("berdl_notebook_utils.get_settings")
    def test_get_default_history_path_settings_error(self, mock_get_settings):
        """Test getting default history path when settings fail."""
        mock_get_settings.side_effect = Exception("Settings error")

        path = get_default_history_path()

        assert path.name == "conversation_history_unknown.json"


class TestConversationHistoryManager:
    """Tests for ConversationHistoryManager class."""

    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_init_with_default_path(self, mock_get_default_path):
        """Test initialization with default path."""
        mock_get_default_path.return_value = Path("/tmp/default_history.json")
        mock_memory = Mock()

        manager = ConversationHistoryManager(mock_memory)

        assert manager.memory == mock_memory
        assert manager.filepath == Path("/tmp/default_history.json")

    def test_init_with_custom_path(self, tmp_path):
        """Test initialization with custom path."""
        mock_memory = Mock()
        custom_path = tmp_path / "custom_history.json"

        manager = ConversationHistoryManager(mock_memory, filepath=custom_path)

        assert manager.filepath == custom_path

    @patch("berdl_notebook_utils.agent.memory.save_conversation_history")
    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_save(self, mock_get_default_path, mock_save):
        """Test save method calls save_conversation_history."""
        mock_get_default_path.return_value = Path("/tmp/history.json")
        mock_memory = Mock()

        manager = ConversationHistoryManager(mock_memory)
        manager.save()

        mock_save.assert_called_once_with(mock_memory, Path("/tmp/history.json"))

    @patch("berdl_notebook_utils.agent.memory.load_conversation_history")
    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_load_when_file_exists(self, mock_get_default_path, mock_load, tmp_path):
        """Test load method calls load_conversation_history when file exists."""
        history_file = tmp_path / "history.json"
        history_file.touch()

        mock_memory = Mock()
        manager = ConversationHistoryManager(mock_memory, filepath=history_file)
        manager.load()

        mock_load.assert_called_once_with(mock_memory, history_file)

    @patch("berdl_notebook_utils.agent.memory.load_conversation_history")
    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_load_when_file_not_exists(self, mock_get_default_path, mock_load, tmp_path, caplog):
        """Test load method logs warning when file doesn't exist."""
        history_file = tmp_path / "nonexistent.json"

        mock_memory = Mock()
        manager = ConversationHistoryManager(mock_memory, filepath=history_file)

        with caplog.at_level(logging.WARNING):
            manager.load()

        mock_load.assert_not_called()
        assert "No saved history found" in caplog.text

    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_clear(self, mock_get_default_path, caplog):
        """Test clear method clears memory."""
        mock_get_default_path.return_value = Path("/tmp/history.json")
        mock_memory = Mock()

        manager = ConversationHistoryManager(mock_memory)

        with caplog.at_level(logging.INFO):
            manager.clear()

        mock_memory.clear.assert_called_once()
        assert "Conversation memory cleared" in caplog.text

    @patch("berdl_notebook_utils.agent.memory.get_default_history_path")
    def test_auto_save_on_exit(self, mock_get_default_path, caplog):
        """Test auto_save_on_exit registers atexit handler."""
        mock_get_default_path.return_value = Path("/tmp/history.json")
        mock_memory = Mock()

        with patch("atexit.register") as mock_atexit:
            manager = ConversationHistoryManager(mock_memory)

            with caplog.at_level(logging.INFO):
                manager.auto_save_on_exit()

            mock_atexit.assert_called_once_with(manager.save)
            assert "Auto-save enabled" in caplog.text
