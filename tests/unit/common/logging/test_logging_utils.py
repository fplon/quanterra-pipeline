"""Unit tests for logging utilities."""

from dataclasses import dataclass
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from tenacity import RetryCallState


@dataclass
class MockNextAction:
    """Mock next action with sleep attribute."""

    sleep: float


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Create a mock logger."""
    with patch("src.common.logging.utils.logger") as mock:
        yield mock


@pytest.fixture
def mock_retry_state() -> MagicMock:
    """Create a mock retry state."""
    state = MagicMock(spec=RetryCallState)
    state.next_action = MockNextAction(sleep=1.5)  # 1.5 seconds wait time
    state.outcome = MagicMock()
    state.outcome.exception.return_value = ValueError("Test error")
    return state


@pytest.fixture
def mock_retry_state_no_action() -> MagicMock:
    """Create a mock retry state with no next action."""
    state = MagicMock(spec=RetryCallState)
    state.next_action = None
    state.outcome = MagicMock()
    state.outcome.exception.return_value = ValueError("Test error")
    return state


@pytest.fixture
def mock_retry_state_no_outcome() -> MagicMock:
    """Create a mock retry state with no outcome."""
    state = MagicMock(spec=RetryCallState)
    state.next_action = MockNextAction(sleep=1.5)
    state.outcome = None
    return state


class TestLoggingUtils:
    """Test suite for logging utilities."""

    def test_log_retry_attempt(self, mock_logger: MagicMock, mock_retry_state: MagicMock) -> None:
        """Test logging of retry attempt with both error and wait time."""
        from src.common.logging.utils import log_retry_attempt

        # Execute
        log_retry_attempt(mock_retry_state)

        # Verify
        mock_logger.warning.assert_called_once()
        args, kwargs = mock_logger.warning.call_args
        assert args[0] == "Request failed with {error}. Retrying in {t:.1f}s..."
        assert isinstance(kwargs["error"], ValueError)
        assert str(kwargs["error"]) == "Test error"
        assert abs(kwargs["t"] - 1.5) < 1e-6

    def test_log_retry_attempt_no_action(
        self, mock_logger: MagicMock, mock_retry_state_no_action: MagicMock
    ) -> None:
        """Test logging of retry attempt with no next action."""
        from src.common.logging.utils import log_retry_attempt

        # Execute
        log_retry_attempt(mock_retry_state_no_action)

        # Verify
        mock_logger.warning.assert_called_once()
        args, kwargs = mock_logger.warning.call_args
        assert args[0] == "Request failed with {error}. Retrying in {t:.1f}s..."
        assert isinstance(kwargs["error"], ValueError)
        assert str(kwargs["error"]) == "Test error"
        assert abs(kwargs["t"]) < 1e-6  # Should be close to 0

    def test_log_retry_attempt_no_outcome(
        self, mock_logger: MagicMock, mock_retry_state_no_outcome: MagicMock
    ) -> None:
        """Test logging of retry attempt with no outcome."""
        from src.common.logging.utils import log_retry_attempt

        # Execute
        log_retry_attempt(mock_retry_state_no_outcome)

        # Verify
        mock_logger.warning.assert_called_once()
        args, kwargs = mock_logger.warning.call_args
        assert args[0] == "Request failed with {error}. Retrying in {t:.1f}s..."
        assert kwargs["error"] == "Unknown error"
        assert abs(kwargs["t"] - 1.5) < 1e-6
