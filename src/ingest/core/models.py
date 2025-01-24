from dataclasses import dataclass
from datetime import datetime
from typing import Any, Generic, TypeVar

T = TypeVar("T")


@dataclass
class DataSource:
    """Metadata about the data source."""

    provider: str
    source_type: str
    timestamp: datetime


@dataclass
class StorageLocation:
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"


@dataclass
class ExtractedData(Generic[T]):
    """Container for extracted data with metadata."""

    data: T
    source: DataSource
    metadata: dict[str, Any]

    def to_json(self) -> dict[str, Any]:
        """Convert to JSON-compatible format."""
        return {
            "data": self.data,
            "metadata": {
                "source": {
                    "provider": self.source.provider,
                    "type": self.source.source_type,
                    "timestamp": self.source.timestamp.isoformat(),
                },
                "extraction_metadata": self.metadata,
            },
        }


@dataclass
class OperationResult:
    """Result of a data operation."""

    success: bool
    location: StorageLocation
    timestamp: datetime
    error_message: str | None = None
