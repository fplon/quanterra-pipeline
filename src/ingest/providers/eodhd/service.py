import gzip
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from google.cloud import storage

from .client import EODHDClient
from .models import EODHDConfig, EODHDData, StorageLocation


@dataclass
class EODHDService:
    """Service for handling EODHD data operations."""

    config: EODHDConfig
    client: Optional[EODHDClient] = None

    def __post_init__(self) -> None:
        if self.client is None:
            self.client = EODHDClient(api_key=self.config.api_key, base_url=self.config.base_url)

    def _get_storage_location(self, data: EODHDData) -> StorageLocation:
        """Generate storage location for data."""
        date_str = data.timestamp.strftime("%Y/%m/%d")
        path = f"raw/eodhd/{date_str}/{data.exchange}/{data.code}.json.gz"
        return StorageLocation(bucket=self.config.bucket_name, path=path)

    async def fetch_and_store_eod_data(self, instrument: str, exchange: str) -> StorageLocation:
        """Fetch EOD data and store it in GCS."""
        # Fetch data
        if self.client is None:
            raise ValueError("Client not initialised")

        raw_data = await self.client.get_eod_data(instrument, exchange)

        # Create data container
        data = EODHDData(
            code=instrument,
            exchange=exchange,
            data=raw_data,
            timestamp=datetime.now(),
            data_type="eod",
        )

        # Store data
        location = self._get_storage_location(data)  # TODO self should be replaced by GCP client
        await self._store_data(data, location)

        return location

    async def _store_data(self, data: EODHDData, location: StorageLocation) -> None:
        """Store data in Google Cloud Storage."""
        # TODO this should be replaced by GCP client - shouldn't be initialised everytime, only the blob
        storage_client = storage.Client()
        bucket = storage_client.bucket(location.bucket)
        blob = bucket.blob(location.path)

        # Convert data to JSON string with metadata
        # TODO make dataclass with serialisation and compression logic?
        json_data = {
            "data": data.data,
            "metadata": {
                "code": data.code,
                "exchange": data.exchange,
                "data_type": data.data_type,  # TODO Enum or literal
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
