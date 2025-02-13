from pathlib import Path

from src.clients.abstract.base_file_client import CSVFileClient


class InteractiveInvestorClient(CSVFileClient):
    """Client for handling Interactive Investor CSV files."""

    def __init__(self, source_path: str | Path) -> None:
        """Initialise the client with source path."""
        super().__init__(
            source_path=Path(source_path) if isinstance(source_path, str) else source_path
        )

    def validate_file_type(self) -> bool:
        """Validate Interactive Investor CSV file format."""
        if not super().validate_file_type():
            return False
        return True
