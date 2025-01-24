from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from .models import ExtractedData, OperationResult, StorageLocation

T = TypeVar("T")


class DataProvider(ABC, Generic[T]):
    """Base interface for data providers."""

    @abstractmethod
    async def fetch_data(self) -> ExtractedData[T]:
        """Fetch data from the source."""
        pass

    @abstractmethod
    async def store_data(self, data: ExtractedData[T]) -> OperationResult:
        """Store the data in the destination."""
        pass

    @abstractmethod
    def get_storage_location(self, data: ExtractedData[T]) -> StorageLocation:
        """Get the storage location for the data."""
        pass
