from datetime import datetime
from typing import Type

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext
from src.ingest.core.processor import BaseProcessor
from src.ingest.data_sources.hargreaves_lansdown.client import HargreavesLansdownClient
from src.ingest.data_sources.hargreaves_lansdown.models import (
    HargreavesLansdownBase,
    HargreavesLansdownClosedPosition,
    HargreavesLansdownConfig,
    HargreavesLansdownPosition,
    HargreavesLansdownTransaction,
)


class HargreavesLansdownProcessor(BaseProcessor):
    """Processor for Hargreaves Lansdown data."""

    def __init__(self, config: HargreavesLansdownConfig):
        self.config = config
        self.hl_client = HargreavesLansdownClient()
        self.storage_client = GCPStorageClient()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        locations = []

        # Process transactions
        if self.config.transactions_source_path:
            locations.extend(
                await self._process_file(
                    self.config.transactions_source_path, HargreavesLansdownTransaction
                )
            )

        # Process positions
        if self.config.positions_source_path:
            locations.extend(
                await self._process_file(
                    self.config.positions_source_path, HargreavesLansdownPosition
                )
            )

        # Process closed positions
        if self.config.closed_positions_source_path:
            locations.extend(
                await self._process_file(
                    self.config.closed_positions_source_path, HargreavesLansdownClosedPosition
                )
            )

        return locations

    async def _process_file(
        self, source_path: str, model_class: Type[HargreavesLansdownBase]
    ) -> list[StorageLocation]:
        self.hl_client.set_source_path(source_path)
        if not self.hl_client.validate_file_type():
            raise ValueError(f"Invalid file format for {source_path}")

        preview_data = self.hl_client.preview_file()
        data = model_class(data=preview_data, timestamp=datetime.now())

        location = StorageLocation(bucket=self.config.bucket_name, path=data.get_storage_path())

        self.storage_client.store_csv_file(
            source_path=source_path,
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )

        return [location]
