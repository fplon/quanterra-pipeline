import csv
from pathlib import Path

from src.clients.file.abstract_file_client import AbstractFileClient


class BaseCSVFileClient(AbstractFileClient):
    """Client for handling CSV files."""

    def __init__(
        self,
        delimiter: str = ",",
        encoding: str = "ISO-8859-1",
        preview_rows: int = 15,
    ) -> None:
        self.delimiter = delimiter
        self.encoding = encoding
        self.preview_rows = preview_rows

    def preview_file(self, source_path: str | Path) -> list[list[str]]:
        """Read first few rows of CSV file as raw rows."""
        with open(source_path, "r", encoding=self.encoding) as f:
            reader = csv.reader(f, delimiter=self.delimiter)
            return [row for _, row in zip(range(self.preview_rows), reader)]

    def validate_file_type(self, source_path: str | Path) -> bool:
        """Validate CSV file format and content."""
        if not Path(source_path).suffix.lower() == ".csv":
            return False
        return True
