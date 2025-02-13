from typing import Any, Dict, List, Protocol, TypeVar, Union


# TODO better implementation?
class JSONValue(Protocol):
    """Protocol for JSON-serialisable values."""

    def __eq__(self, __other: object) -> bool: ...


JSONPrimitive = Union[str, int, float, bool, None]

JSONType = Union[JSONPrimitive, Dict[str, Any], List[Any]]

T = TypeVar("T", bound=JSONType)
