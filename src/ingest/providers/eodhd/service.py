from dataclasses import dataclass
from datetime import datetime

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType

from .client import EODHDClient
from .models import (
    EODHDConfig,
    EODHDData,
    EODHDExchangeData,
    EODHDExchangeSymbolData,
    StorageLocation,
)


@dataclass
class EODHDService:
    """Service for handling EODHD data operations."""

    config: EODHDConfig
    eodhd_client: EODHDClient | None = None
    storage_client: GCPStorageClient | None = None

    def __post_init__(self) -> None:
        if self.eodhd_client is None:
            self.eodhd_client = EODHDClient(
                api_key=self.config.api_key, base_url=self.config.base_url
            )
        if self.storage_client is None:
            self.storage_client = GCPStorageClient()

    def _get_storage_location(
        self, data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData
    ) -> StorageLocation:
        """Generate storage location for data."""
        return StorageLocation(bucket=self.config.bucket_name, path=data.get_storage_path())

    async def fetch_and_store_exchange_symbol_data(self, exchange: str) -> StorageLocation:
        """Fetch and store data for a specific exchange."""
        if self.eodhd_client is None:
            raise ValueError("Client not initialised")

        # TODO Not currently handling/using asset type or delisted
        raw_data = await self.eodhd_client.get_exchange_symbols(exchange)

        # Create data container
        data = EODHDExchangeSymbolData(
            exchange=exchange,
            data=raw_data,
            timestamp=datetime.now(),
        )

        # Store data
        location = self._get_storage_location(data)
        await self._store_data(data, location)

        return location

    async def fetch_and_store_exchange_data(self) -> StorageLocation:
        """Fetch and store data for a specific exchange."""
        if self.eodhd_client is None:
            raise ValueError("Client not initialised")

        raw_data = await self.eodhd_client.get_exchanges()

        # Create data container
        data = EODHDExchangeData(
            data=raw_data,
            timestamp=datetime.now(),
        )

        # Store data
        location = self._get_storage_location(data)
        await self._store_data(data, location)

        return location

    async def fetch_and_store_data(
        self, instrument: str, exchange: str, data_type: str = "eod"
    ) -> StorageLocation:
        """Fetch and store data for a specific instrument and data type."""
        if self.eodhd_client is None:
            raise ValueError("Client not initialised")

        # Fetch data based on type
        raw_data = await self._fetch_data_by_type(instrument, exchange, data_type)

        # Create data container
        data = EODHDData(
            code=instrument,
            exchange=exchange,
            data=raw_data,
            timestamp=datetime.now(),
            data_type=data_type,
        )

        # Store data
        location = self._get_storage_location(data)
        await self._store_data(data, location)

        return location

    async def _fetch_data_by_type(self, instrument: str, exchange: str, data_type: str) -> JSONType:
        """Fetch data based on type."""
        if self.eodhd_client is None:
            raise ValueError("Client not initialised")

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

    async def _store_data(
        self,
        data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData,
        location: StorageLocation,
    ) -> None:
        """Store data in Google Cloud Storage."""
        if self.storage_client is None:
            raise RuntimeError("Storage client not initialized")

        # Convert data to JSON string with metadata
        if isinstance(data, EODHDData):
            json_data = {
                "data": data.data,
                "metadata": {
                    "code": data.code,
                    "exchange": data.exchange,
                    "data_type": data.data_type,
                    "timestamp": data.timestamp.isoformat(),
                },
            }
        elif isinstance(data, EODHDExchangeData):
            json_data = {
                "data": data.data,
                "metadata": {
                    "data_type": "exchanges-list",
                    "timestamp": data.timestamp.isoformat(),
                },
            }
        elif isinstance(data, EODHDExchangeSymbolData):
            json_data = {
                "data": data.data,
                "metadata": {
                    "exchange": data.exchange,
                    "data_type": "exchange-symbol-list",
                    "timestamp": data.timestamp.isoformat(),
                },
            }

        # Store data using the GCP client
        self.storage_client.store_json_data(
            data=json_data,
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )
