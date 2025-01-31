import gzip
import json
import shutil
from pathlib import Path
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

    def store_csv_file(
        self,
        source_path: str | Path,
        bucket_name: str,
        blob_path: str,
        compress: bool = True,
    ) -> None:
        """Store a CSV file in GCP Storage with optional compression.

        Args:
            source_path: Path to the source CSV file
            bucket_name: Name of the GCP bucket
            blob_path: Path within the bucket
            compress: Whether to compress the file using gzip
        """
        if self._client is None:
            raise RuntimeError("GCP Storage client not initialised")

        source_path = Path(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if compress:
            # Create a temporary compressed file
            temp_compressed = Path(str(source_path) + ".gz")
            try:
                with open(source_path, "rb") as f_in:
                    with gzip.open(temp_compressed, "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                blob.content_encoding = "gzip"
                blob.content_type = "text/csv"
                blob.upload_from_filename(str(temp_compressed))

            finally:
                # Clean up temporary file
                if temp_compressed.exists():
                    temp_compressed.unlink()
        else:
            blob.content_type = "text/csv"
            blob.upload_from_filename(str(source_path))
