from datetime import datetime
from typing import Any

import pandas as pd

from src.common.types import JSONType


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
