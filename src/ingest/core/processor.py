from abc import ABC, abstractmethod
from typing import List, Optional

from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext


class BaseProcessor(ABC):
    """Base interface for all data processors."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for the processor."""
        pass

    @abstractmethod
    async def process(self, context: PipelineContext) -> Optional[List[StorageLocation]]:
        """Execute the processor with given context."""
        pass
