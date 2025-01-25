import gzip
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from google.cloud import storage

from .client import EODHDClient
from .models import (
    EODHDConfig,
    EODHDData,
    EODHDExchangeData,
    EODHDExchangeSymbolData,
    StorageLocation,
)
from .types import JSONType


@dataclass
class EODHDService:
    """Service for handling EODHD data operations."""

    config: EODHDConfig
    client: Optional[EODHDClient] = None

    def __post_init__(self) -> None:
        if self.client is None:
            self.client = EODHDClient(api_key=self.config.api_key, base_url=self.config.base_url)

    def _get_storage_location(
        self, data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData
    ) -> StorageLocation:
        """Generate storage location for data."""
        return StorageLocation(bucket=self.config.bucket_name, path=data.get_storage_path())

    async def fetch_and_store_exchange_symbol_data(self, exchange: str) -> StorageLocation:
        """Fetch and store data for a specific exchange."""
        if self.client is None:
            raise ValueError("Client not initialised")

        # TODO Not currently handling/using asset type or delisted
        raw_data = await self.client.get_exchange_symbols(exchange)

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
        if self.client is None:
            raise ValueError("Client not initialised")

        raw_data = await self.client.get_exchanges()

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
        if self.client is None:
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
        if self.client is None:
            raise ValueError("Client not initialised")

        if data_type == "eod":
            return await self.client.get_eod_data(instrument, exchange)
        elif data_type == "dividends":
            return await self.client.get_dividends(instrument, exchange)
        elif data_type == "splits":
            return await self.client.get_splits(instrument, exchange)
        elif data_type == "fundamentals":
            return await self.client.get_fundamentals(instrument, exchange)
        elif data_type == "news":
            return await self.client.get_news(instrument, exchange)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

    async def _store_data(
        self,
        data: EODHDData | EODHDExchangeData | EODHDExchangeSymbolData,
        location: StorageLocation,
    ) -> None:
        """Store data in Google Cloud Storage."""
        # TODO this should be replaced by GCP client - shouldn't be initialised everytime, only the blob
        storage_client = storage.Client()
        bucket = storage_client.bucket(location.bucket)
        blob = bucket.blob(location.path)

        # Convert data to JSON string with metadata
        # TODO make dataclass with serialisation and compression logic?
        if isinstance(data, EODHDData):
            json_data = {
                "data": data.data,
                "metadata": {
                    "code": data.code,
                    "exchange": data.exchange,
                    "data_type": data.data_type,  # TODO Enum or literal
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

        # Proper serialization
        json_string = json.dumps(json_data)

        # Compress
        compressed_data = gzip.compress(json_string.encode("utf-8"))

        # Set blob metadata
        blob.content_encoding = "gzip"
        blob.content_type = "text/plain"

        # Upload compressed data
        blob.upload_from_string(compressed_data)
