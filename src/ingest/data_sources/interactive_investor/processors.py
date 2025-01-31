from datetime import datetime

from loguru import logger

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext
from src.ingest.core.processor import BaseProcessor
from src.ingest.data_sources.interactive_investor.client import InteractiveInvestorClient
from src.ingest.data_sources.interactive_investor.models import (
    InteractiveInvestorConfig,
    InteractiveInvestorTransaction,
)


class InteractiveInvestorProcessor(BaseProcessor):
    """Processor for Interactive Investor transaction data."""

    def __init__(self, config: InteractiveInvestorConfig):
        self.config = config
        self.ii_client = InteractiveInvestorClient(source_path=config.source_path)
        self.storage_client = GCPStorageClient()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        """Process Interactive Investor transaction data."""
        logger.info("Processing Interactive Investor transaction data")
        try:
            # Validate file format
            if not self.ii_client.validate_file_type():
                raise ValueError("Invalid Interactive Investor CSV file format")

            # Preview data for pydantic validation
            preview_data = self.ii_client.preview_file()
            data = InteractiveInvestorTransaction(
                data=preview_data,
                timestamp=datetime.now(),
            )

            # Get storage location
            location = StorageLocation(
                bucket=self.config.bucket_name,
                path=data.get_storage_path(),
            )

            # Store the CSV file with compression
            self.storage_client.store_csv_file(
                source_path=self.ii_client.source_path,
                bucket_name=location.bucket,
                blob_path=location.path,
                compress=True,
            )

            logger.success(f"Stored Interactive Investor transaction data at: {location}")
            return [location]

        except Exception:
            logger.exception("Error processing Interactive Investor transaction data")
            raise
