"""Unit tests for logging configuration."""

import sys
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

from src.common.logging.config import LoggerConfig, setup_logger


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Create a mock logger."""
    with patch("src.common.logging.config.logger") as mock:
        yield mock


@pytest.fixture
def mock_path() -> Generator[MagicMock, None, None]:
    """Create a mock Path."""
    with patch("src.common.logging.config.Path") as mock:
        yield mock


@pytest.fixture
def reset_logger_config() -> Generator[None, None, None]:
    """Reset LoggerConfig singleton state between tests."""
    LoggerConfig._instance = None
    LoggerConfig._initialised = False
    yield
    LoggerConfig._instance = None
    LoggerConfig._initialised = False


class TestLoggerConfig:
    """Test suite for LoggerConfig."""

    def test_singleton_pattern(self, reset_logger_config: None) -> None:
        """Test that LoggerConfig follows singleton pattern."""
        config1 = LoggerConfig()
        config2 = LoggerConfig()
        assert config1 is config2
        assert LoggerConfig._instance is config1

    def test_init_creates_logs_directory(
        self, reset_logger_config: None, mock_path: MagicMock
    ) -> None:
        """Test that logs directory is created during initialization."""
        LoggerConfig()
        mock_path.assert_called_once_with("logs")
        mock_path.return_value.mkdir.assert_called_once_with(exist_ok=True)

    def test_configure_removes_default_handler(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that default handler is removed during configuration."""
        config = LoggerConfig()
        config.configure("test_app")
        mock_logger.remove.assert_called_once()

    def test_configure_adds_file_handler(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that file handler is added with correct parameters."""
        config = LoggerConfig()
        config.configure("test_app")

        # First call to add() should be for file handler
        args, kwargs = mock_logger.add.call_args_list[0]
        assert args[0] == "logs/test_app_{time}.log"
        assert kwargs["rotation"] == "1 day"
        assert kwargs["retention"] == "1 week"
        assert kwargs["compression"] == "zip"
        assert kwargs["level"] == "INFO"
        assert kwargs["backtrace"] is True
        assert kwargs["diagnose"] is True
        assert "{time:YYYY-MM-DD HH:mm:ss}" in kwargs["format"]
        assert "{level: <8}" in kwargs["format"]
        assert "{message}" in kwargs["format"]

    def test_configure_adds_console_handler(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that console handler is added with correct parameters."""
        config = LoggerConfig()
        config.configure("test_app")

        # Second call to add() should be for console handler
        args, kwargs = mock_logger.add.call_args_list[1]
        assert args[0] is sys.stderr
        assert kwargs["colorize"] is True
        assert kwargs["level"] == "INFO"
        assert kwargs["backtrace"] is True
        assert kwargs["diagnose"] is True
        assert "<green>{time:HH:mm:ss}</green>" in kwargs["format"]
        assert "<level>{level: <8}</level>" in kwargs["format"]
        assert "<level>{message}</level>" in kwargs["format"]

    def test_configure_only_runs_once(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that configuration only happens once."""
        config = LoggerConfig()
        config.configure("test_app")
        config.configure("another_app")  # Should not trigger configuration again

        assert mock_logger.remove.call_count == 1
        assert mock_logger.add.call_count == 2  # One for file, one for console

    def test_configure_with_default_app_name(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test configuration with default app name."""
        config = LoggerConfig()
        config.configure()

        args, _ = mock_logger.add.call_args_list[0]
        assert args[0] == "logs/app_{time}.log"


class TestSetupLogger:
    """Test suite for setup_logger function."""

    def test_setup_logger_creates_and_configures(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that setup_logger creates and configures LoggerConfig instance."""
        setup_logger("test_app")
        assert LoggerConfig._instance is not None
        assert LoggerConfig._initialised is True
        assert mock_logger.add.call_count == 2  # One for file, one for console

    def test_setup_logger_reuses_existing_config(
        self, reset_logger_config: None, mock_logger: MagicMock
    ) -> None:
        """Test that setup_logger reuses existing LoggerConfig instance."""
        config1 = LoggerConfig()
        setup_logger("test_app")
        assert LoggerConfig._instance is config1
