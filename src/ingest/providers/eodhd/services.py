from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from loguru import logger

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType
from src.ingest.providers.eodhd.client import EODHDClient
from src.ingest.providers.eodhd.models import (
    EODHDConfig,
    EODHDData,
    EODHDExchangeData,
    EODHDExchangeSymbolData,
    StorageLocation,
)


class EODIngestionService(ABC):
    """Base service for EODHD data ingestion."""

    def __init__(self, config: EODHDConfig):
        self.config = config
        # TODO inject clients
        self.eodhd_client = EODHDClient(api_key=config.api_key, base_url=config.base_url)
        self.storage_client = GCPStorageClient()

    @abstractmethod
    async def process(self) -> List[StorageLocation]:
        """Execute the ingestion process."""
        pass

    async def _store_data(
        self,
        data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData,
        location: StorageLocation,
    ) -> None:
        """Store data in Google Cloud Storage."""
        if self.storage_client is None:
            raise RuntimeError("Storage client not initialized")

        json_data = self._prepare_json_data(data)

        # Store data using the GCP client
        self.storage_client.store_json_data(
            data=json_data,
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )

    def _prepare_json_data(
        self, data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData
    ) -> dict:  # type: ignore # FIXME creates upload types
        """Prepare JSON data for storage with appropriate metadata."""
        if isinstance(data, EODHDData):
            return {
                "data": data.data,
                "metadata": {
                    "code": data.code,
                    "exchange": data.exchange,
                    "data_type": data.data_type,
                    "timestamp": data.timestamp.isoformat(),
                },
            }
        elif isinstance(data, EODHDExchangeData):
            return {
                "data": data.data,
                "metadata": {
                    "data_type": "exchanges-list",
                    "timestamp": data.timestamp.isoformat(),
                },
            }
        else:  # EODHDExchangeSymbolData
            return {
                "data": data.data,
                "metadata": {
                    "exchange": data.exchange,
                    "data_type": "exchange-symbol-list",
                    "timestamp": data.timestamp.isoformat(),
                },
            }


class ExchangeDataService(EODIngestionService):
    """Service for processing exchange-level data."""

    async def process(self) -> List[StorageLocation]:
        logger.info("Processing exchange data")
        try:
            raw_data = await self.eodhd_client.get_exchanges()
            data = EODHDExchangeData(
                data=raw_data,
                timestamp=datetime.now(),
            )
            location = StorageLocation(bucket=self.config.bucket_name, path=data.get_storage_path())
            await self._store_data(data, location)
            logger.success(f"Stored exchange data at: {location}")
            return [location]
        except Exception:
            logger.exception("Error processing exchange data")
            raise


class ExchangeSymbolService(EODIngestionService):
    """Service for processing exchange-symbol data."""

    async def process(self) -> List[StorageLocation]:
        locations = []
        for exchange in self.config.exchanges:
            try:
                logger.info(f"Processing exchange-symbol data for {exchange}")
                raw_data = await self.eodhd_client.get_exchange_symbols(exchange)
                data = EODHDExchangeSymbolData(
                    exchange=exchange,
                    data=raw_data,
                    timestamp=datetime.now(),
                )
                location = StorageLocation(
                    bucket=self.config.bucket_name, path=data.get_storage_path()
                )
                await self._store_data(data, location)
                logger.success(f"Stored exchange-symbol data at: {location}")
                locations.append(location)
            except Exception:
                logger.exception(f"Error processing exchange-symbol data for {exchange}")
                raise
        return locations


class InstrumentDataService(EODIngestionService):
    """Service for processing instrument-specific data."""

    def __init__(self, config: EODHDConfig, data_types: List[str]):
        super().__init__(config)
        self.data_types = data_types

    async def process(self) -> List[StorageLocation]:
        locations = []
        instrument_pairs = [(i.split(".", 1)) for i in self.config.instruments]

        for instrument, exchange in instrument_pairs:
            for data_type in self.data_types:
                try:
                    logger.info(f"Processing {data_type} data for {exchange}/{instrument}")
                    raw_data = await self._fetch_data_by_type(instrument, exchange, data_type)
                    data = EODHDData(
                        code=instrument,
                        exchange=exchange,
                        data=raw_data,
                        timestamp=datetime.now(),
                        data_type=data_type,
                    )
                    location = StorageLocation(
                        bucket=self.config.bucket_name, path=data.get_storage_path()
                    )
                    await self._store_data(data, location)
                    logger.success(f"Stored {data_type} data at: {location}")
                    locations.append(location)
                except Exception:
                    logger.exception(
                        f"Error processing {data_type} data for {exchange}/{instrument}"
                    )
                    continue
        return locations

    async def _fetch_data_by_type(self, instrument: str, exchange: str, data_type: str) -> JSONType:
        """Fetch data based on type."""
        if data_type == "eod":
            return await self.eodhd_client.get_eod_data(instrument, exchange)
        elif data_type == "dividends":
            return await self.eodhd_client.get_dividends(instrument, exchange)
        elif data_type == "splits":
            return await self.eodhd_client.get_splits(instrument, exchange)
        elif data_type == "fundamentals":
            return await self.eodhd_client.get_fundamentals(instrument, exchange)
        elif data_type == "news":
            return await self.eodhd_client.get_news(instrument, exchange)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
