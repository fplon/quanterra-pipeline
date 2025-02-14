from datetime import datetime
from typing import Any

import pandas as pd
from loguru import logger
from tenacity import RetryCallState

from src.models.data.json_objects import JSONType  # TODO better implementation


def convert_to_json_safe(obj: Any) -> JSONType:
    """Convert problematic data types to JSON-safe formats."""
    if pd.isna(obj):
        return None
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, (int, float, str, bool)):
        return obj
    elif isinstance(obj, dict):
        return {str(k): convert_to_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_json_safe(v) for v in obj]
    return str(obj)


# TODO replace with prefect retry?
def log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log retry attempt with error and wait time."""
    wait = retry_state.next_action.sleep if retry_state.next_action else 0
    error = retry_state.outcome.exception() if retry_state.outcome else "Unknown error"
    logger.warning(
        "Request failed with {error}. Retrying in {t:.1f}s...",
        error=error,
        t=wait,
    )
