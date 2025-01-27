import gzip
import json
from typing import Optional

from google.cloud import storage
from loguru import logger

from ..types import JSONType


class GCPStorageClient:
    """Singleton class for GCP Storage operations."""

    _instance: Optional["GCPStorageClient"] = None
    _client: Optional[storage.Client] = None

    def __new__(cls) -> "GCPStorageClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """Initialise the GCP Storage client if not already initialised."""
        if self._client is None:
            logger.debug("Initialising new GCP Storage client")
            self._client = storage.Client()

    def store_json_data(
        self,
        data: JSONType,
        bucket_name: str,
        blob_path: str,
        compress: bool = True,
    ) -> None:
        """Store JSON data in GCP Storage.

        Args:
            data: Dictionary to store
            bucket_name: Name of the GCP bucket
            blob_path: Path within the bucket
            compress: Whether to compress the data using gzip
        """
        if self._client is None:
            raise RuntimeError("GCP Storage client not initialised")

        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Serialise data
        json_string = json.dumps(data)

        if compress:
            # Compress data
            content = gzip.compress(json_string.encode("utf-8"))
            blob.content_encoding = "gzip"
            blob.content_type = "text/plain"
        else:
            content = json_string.encode("utf-8")
            blob.content_type = "application/json"

        # Upload data
        blob.upload_from_string(content)
