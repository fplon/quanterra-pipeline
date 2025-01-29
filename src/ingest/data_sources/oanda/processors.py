from abc import abstractmethod
from datetime import datetime

from loguru import logger

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext
from src.ingest.core.processor import BaseProcessor
from src.ingest.data_sources.oanda.client import OANDAClient
from src.ingest.data_sources.oanda.models import (
    BaseOANDAData,
    CandlesData,
    InstrumentsData,
    OANDAConfig,
)


class OANDAIngestionProcessor(BaseProcessor):
    """Base processor for OANDA data ingestion."""

    def __init__(self, config: OANDAConfig):
        self.config = config
        self.oanda_client = OANDAClient(
            api_key=config.api_key,
            base_url=config.base_url,
            account_id=config.account_id,
        )
        self.storage_client = GCPStorageClient()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        """Execute the ingestion process."""
        pass

    async def _store_data(self, data: BaseOANDAData, location: StorageLocation) -> None:
        """Store data in Google Cloud Storage."""
        if self.storage_client is None:
            raise RuntimeError("Storage client not initialised")

        self.storage_client.store_json_data(
            data=data.to_json(),
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )


class InstrumentsProcessor(OANDAIngestionProcessor):
    """Processor for fetching and storing available instruments."""

    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        logger.info("Processing instruments data")
        try:
            raw_data = await self.oanda_client.get_instruments()
            data = InstrumentsData(
                data=raw_data,
                timestamp=datetime.now(),
            )
            location = StorageLocation(bucket=self.config.bucket_name, path=data.get_storage_path())
            await self._store_data(data, location)
            logger.success(f"Stored instruments data at: {location}")

            # Store instruments in context for downstream processors
            context.shared_state["available_instruments"] = data.get_instruments_list()
            return [location]
        except Exception:
            logger.exception("Error processing instruments data")
            raise


class CandlesProcessor(OANDAIngestionProcessor):
    """Processor for fetching and storing candles data."""

    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        locations = []
        # Use instruments from context if available, otherwise use config # TODO tidy up
        instruments = (
            context.shared_state.get("available_instruments", [])
            if not self.config.instruments
            else self.config.instruments
        )

        for instrument in instruments:
            try:
                logger.info(f"Processing candles data for {instrument}")
                raw_data = await self.oanda_client.get_candles(
                    instrument=instrument,
                    granularity=self.config.granularity,
                    count=self.config.count,
                    price=self.config.price,
                )
                data = CandlesData(
                    instrument=instrument,
                    data=raw_data,
                    timestamp=datetime.now(),
                )
                location = StorageLocation(
                    bucket=self.config.bucket_name,
                    path=data.get_storage_path(),
                )
                await self._store_data(data, location)
                logger.success(f"Stored candles data at: {location}")
                locations.append(location)
            except Exception:
                logger.exception(f"Error processing candles data for {instrument}")
                raise
        return locations
