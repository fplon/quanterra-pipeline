import gzip
import json
import shutil
from pathlib import Path
from typing import Optional

from google.cloud import storage
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from src.models.data.json_objects import JSONType  # TODO better implementation


class GCPStorageClient:
    """Singleton class for GCP Storage operations."""

    _instance: Optional["GCPStorageClient"] = None
    _client: Optional[storage.Client] = None
    _credentials: Optional[Credentials | ServiceAccountCredentials] = None

    def __new__(
        cls, credentials: Optional[Credentials | ServiceAccountCredentials] = None
    ) -> "GCPStorageClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._credentials = credentials
        return cls._instance

    def __init__(
        self, credentials: Optional[Credentials | ServiceAccountCredentials] = None
    ) -> None:
        """Initialise the GCP Storage client if not already initialised.

        Args:
            credentials: Optional GCP credentials. If not provided, will use Application Default Credentials.
        """
        if self._client is None:
            self._client = storage.Client(credentials=self._credentials)

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

    # TODO refactor - repo pattern
    def store_csv_file_from_blob(
        self,
        source_bucket_name: str,
        source_blob_path: str,
        target_bucket_name: str,
        target_blob_path: str,
        compress: bool = True,
    ) -> None:
        """Move a CSV file from one blob location to another with optional compression.

        Args:
            source_bucket_name: Name of the source GCP bucket
            source_blob_path: Path of the source blob
            target_bucket_name: Name of the target GCP bucket
            target_blob_path: Path for the target blob
            compress: Whether to compress the file using gzip
        """
        if self._client is None:
            raise RuntimeError("GCP Storage client not initialised")

        source_bucket = self._client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_path)
        target_bucket = self._client.bucket(target_bucket_name)
        target_blob = target_bucket.blob(target_blob_path)

        # Download content as bytes to preserve the original format
        content = source_blob.download_as_bytes()

        if compress:
            # Compress the content if it's not already compressed
            if not source_blob.content_encoding == "gzip":
                content = gzip.compress(content)
                target_blob.content_encoding = "gzip"

        # Set the content type to match the source, defaulting to text/csv if not set
        target_blob.content_type = source_blob.content_type or "text/csv"

        # Upload the content
        target_blob.upload_from_string(
            content,
            content_type=target_blob.content_type,
        )
