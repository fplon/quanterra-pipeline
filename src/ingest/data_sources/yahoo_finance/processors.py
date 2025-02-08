from datetime import datetime

from loguru import logger

from src.common.gcp.client import GCPStorageClient
from src.common.json_utils import convert_to_json_safe
from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext
from src.ingest.core.processor import BaseProcessor
from src.ingest.data_sources.yahoo_finance.client import YahooFinanceClient
from src.ingest.data_sources.yahoo_finance.models import (
    YahooFinanceConfig,
    YahooFinanceData,
)


class YahooFinanceProcessor(BaseProcessor):
    """Processor for Yahoo Finance data."""

    def __init__(self, config: YahooFinanceConfig):
        """Initialise processor with config."""
        self.config = config
        self.storage_client = GCPStorageClient(credentials=config.gcp_credentials)
        self.yf_client = YahooFinanceClient()

    @property
    def name(self) -> str:
        return "YahooFinanceProcessor"

    def _store_data(self, data: YahooFinanceData, location: StorageLocation) -> None:
        """Store data in GCP bucket."""
        self.storage_client.store_json_data(
            bucket_name=location.bucket,
            blob_path=location.path,
            data=convert_to_json_safe(data.data),
            compress=True,
        )

    async def process(self, context: PipelineContext) -> list[StorageLocation]:
        """Process Yahoo Finance data."""
        locations = []
        try:
            with self.yf_client as client:
                # Get ticker data
                logger.info("Processing ticker data")
                ticker_data = client.get_tickers_data(self.config.tickers)
                data = YahooFinanceData(
                    data=ticker_data, timestamp=datetime.now(), data_type="tickers"
                )
                location = StorageLocation(
                    bucket=self.config.bucket_name, path=data.get_storage_path()
                )
                self._store_data(data, location)
                locations.append(location)
                logger.success(f"Stored ticker data at: {location}")

                # Get market data
                logger.info("Processing market data")
                market_data = client.get_market_data(self.config.tickers)
                data = YahooFinanceData(
                    data=market_data, timestamp=datetime.now(), data_type="market"
                )
                location = StorageLocation(
                    bucket=self.config.bucket_name, path=data.get_storage_path()
                )
                self._store_data(data, location)
                locations.append(location)
                logger.success(f"Stored market data at: {location}")

        except Exception:
            logger.exception("Error processing Yahoo Finance data")
            raise

        return locations
