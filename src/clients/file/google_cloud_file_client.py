import csv
from io import StringIO
from pathlib import Path

from src.clients.file.base_csv_client import BaseCSVFileClient
from src.clients.google_cloud_storage_client import GCPStorageClient


class GoogleCloudFileClient(BaseCSVFileClient):
    """Client for handling CSV files stored in Google Cloud Storage."""

    def __init__(
        self,
        bucket_name: str,
        delimiter: str = ",",
        encoding: str = "ISO-8859-1",
        preview_rows: int = 15,
    ) -> None:
        super().__init__(delimiter=delimiter, encoding=encoding, preview_rows=preview_rows)
        self.bucket_name = bucket_name
        self.storage_client = GCPStorageClient()
        self.bucket = self.storage_client._client.bucket(self.bucket_name)

    def preview_file(self, source_path: str | Path) -> list[list[str]]:
        """Read first few rows of CSV file from Google Cloud Storage."""
        blob = self.bucket.blob(str(source_path))

        # Download the content as a string
        content = blob.download_as_text(encoding=self.encoding)

        # Use StringIO to create a file-like object from the string
        # TODO this is redundant? File already read in in content variable
        csv_file = StringIO(content)
        reader = csv.reader(csv_file, delimiter=self.delimiter)

        # Get the preview rows
        return [row for _, row in zip(range(self.preview_rows), reader)]

    def validate_file_type(self, source_path: str | Path) -> bool:
        """Validate CSV file format and content in Google Cloud Storage."""
        # First check if the path has a .csv extension
        if not Path(source_path).suffix.lower() == ".csv":
            return False

        # Then verify the file exists in the bucket
        blob = self.bucket.blob(str(source_path))
        return blob.exists()
