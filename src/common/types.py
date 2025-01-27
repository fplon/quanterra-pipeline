from typing import Mapping, Protocol, Sequence, TypeVar, Union


class JSONValue(Protocol):
    """Protocol for JSON-serialisable values."""

    def __eq__(self, __other: object) -> bool: ...


JSONPrimitive = Union[str, int, float, bool, None]
JSONObject = Mapping[str, "JSONType"]
JSONArray = Sequence["JSONType"]
JSONType = Union[JSONPrimitive, JSONObject, JSONArray]

T = TypeVar("T", bound=JSONType)
