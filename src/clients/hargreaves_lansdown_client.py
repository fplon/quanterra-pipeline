from pathlib import Path

from src.clients.abstract.base_file_client import CSVFileClient


class HargreavesLansdownClient(CSVFileClient):
    """Client for handling Hargreaves Lansdown CSV files."""

    def __init__(self) -> None:
        """Initialise the client without source path."""
        super().__init__(source_path=Path(""))
        self._current_source_path: Path = Path("")

    def set_source_path(self, source_path: str | Path) -> None:
        """Set the current source path for processing."""
        self._current_source_path = (
            Path(source_path) if isinstance(source_path, str) else source_path
        )
        self.source_path = self._current_source_path

    def validate_file_type(self) -> bool:
        """Validate Hargreaves Lansdown CSV file format."""
        if not super().validate_file_type():
            return False
        return True
