from abc import ABC, abstractmethod
from csv import DictReader
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loguru import logger


@dataclass
class BaseFileClient(ABC):
    """Base client for handling file-based data sources."""

    source_path: Path

    def __post_init__(self) -> None:
        """Validate source path exists."""
        if not self.source_path.exists():
            raise FileNotFoundError(f"Source path does not exist: {self.source_path}")

    @abstractmethod
    def preview_file(self) -> list[dict[str, Any]]:
        """Read first few rows of the file for validation."""
        pass

    @abstractmethod
    def validate_file_type(self) -> bool:
        """Validate file format and content."""
        pass


@dataclass
class CSVFileClient(BaseFileClient):
    """Client for handling CSV files."""

    delimiter: str = ","
    encoding: str = "ISO-8859-1"
    preview_rows: int = 5

    def preview_file(self) -> list[dict[str, Any]]:
        """Read first few rows of CSV file for validation."""
        try:
            with open(self.source_path, "r", encoding=self.encoding) as f:
                reader = DictReader(f, delimiter=self.delimiter)
                return [row for _, row in zip(range(self.preview_rows), reader)]
        except Exception as e:
            logger.error(f"Error reading CSV file {self.source_path}: {str(e)}")
            raise

    def validate_file_type(self) -> bool:
        """Validate CSV file format and content."""
        if not self.source_path.suffix.lower() == ".csv":
            return False
        return True
