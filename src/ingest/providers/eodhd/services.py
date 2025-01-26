from abc import ABC, abstractmethod
from datetime import datetime
from typing import Awaitable, Callable, List

from loguru import logger

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType
from src.ingest.providers.eodhd.client import EODHDClient
from src.ingest.providers.eodhd.models import (
    BaseEODHDData,
    EODHDConfig,
    ExchangeData,
    ExchangeSymbolData,
    InstrumentData,
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

    async def _store_data(self, data: BaseEODHDData, location: StorageLocation) -> None:
        """Store data in Google Cloud Storage."""
        if self.storage_client is None:
            raise RuntimeError("Storage client not initialized")

        self.storage_client.store_json_data(
            data=data.to_json(),
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )


class ExchangeDataService(EODIngestionService):
    """Service for processing exchange-level data."""

    async def process(self) -> List[StorageLocation]:
        logger.info("Processing exchange data")
        try:
            raw_data = await self.eodhd_client.get_exchanges()
            data = ExchangeData(
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
                data = ExchangeSymbolData(
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

    def __init__(self, config: EODHDConfig):
        super().__init__(config)
        self._data_type_handlers: dict[str, Callable[[str, str], Awaitable[JSONType]]] = {
            "eod": self.eodhd_client.get_eod_data,
            "dividends": self.eodhd_client.get_dividends,
            "splits": self.eodhd_client.get_splits,
            "fundamentals": self.eodhd_client.get_fundamentals,
            "news": self.eodhd_client.get_news,
        }

    async def process(self) -> List[StorageLocation]:
        locations = []
        instrument_pairs = [(i.split(".", 1)) for i in self.config.instruments]

        for instrument, exchange in instrument_pairs:
            for data_type in self._data_type_handlers.keys():
                try:
                    logger.info(f"Processing {data_type} data for {exchange}/{instrument}")
                    raw_data = await self._fetch_data_by_type(instrument, exchange, data_type)
                    data = InstrumentData(
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
        handler = self._data_type_handlers.get(data_type)
        if handler is None:
            raise ValueError(f"Unsupported data type: {data_type}")
        return await handler(instrument, exchange)
