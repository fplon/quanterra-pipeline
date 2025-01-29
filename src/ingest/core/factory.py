from abc import ABC, abstractmethod
from typing import Any, TypeVar

from src.ingest.core.manifest import ProcessorType
from src.ingest.core.processor import BaseProcessor

T = TypeVar("T", bound=BaseProcessor)


class ProcessorFactory(ABC):
    """Base interface for processor factories."""

    @abstractmethod
    def create_processor(
        self,
        processor_type: ProcessorType,
        config: Any,
    ) -> BaseProcessor:
        """Create a processor instance based on the specified type.

        Args:
            processor_type: Type of processor to create from manifest
            config: Configuration for the processor

        Returns:
            An instance of a processor

        Raises:
            ValueError: If processor_type is not supported
        """
        pass
