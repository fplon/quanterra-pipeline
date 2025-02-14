from abc import ABC, abstractmethod


class AbstractFileClient(ABC):
    """Base client for handling file-based data sources."""

    @abstractmethod
    def preview_file(self) -> list[list[str]]:
        """Read first few rows of the file for validation."""
        pass

    @abstractmethod
    def validate_file_type(self) -> bool:
        """Validate file format and content."""
        pass
