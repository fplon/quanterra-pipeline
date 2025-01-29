from typing import Any, Dict, List, Protocol, TypeVar, Union


class JSONValue(Protocol):
    """Protocol for JSON-serialisable values."""

    def __eq__(self, __other: object) -> bool: ...


# Simple JSON primitive types
JSONPrimitive = Union[str, int, float, bool, None]

# Use Dict[str, Any] and List[Any] instead of recursive definitions
# This avoids the infinite recursion while still maintaining type safety for JSON
JSONType = Union[JSONPrimitive, Dict[str, Any], List[Any]]

T = TypeVar("T", bound=JSONType)
