from loguru import logger
from tenacity import RetryCallState


def log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempt with error and wait time."""
    wait = retry_state.next_action.sleep if retry_state.next_action else 0
    error = retry_state.outcome.exception() if retry_state.outcome else "Unknown error"
    logger.warning(
        "Request failed with {error}. Retrying in {t:.1f}s...",
        error=error,
        t=wait,
    )
