from datetime import datetime

from pydantic import BaseModel

from src.models.data.json_objects import JSONType  # TODO better implementation


class BaseOANDAData(BaseModel):
    """Base class for all OANDA data types."""

    data: JSONType
    timestamp: datetime
    data_type: str

    def to_json(self) -> dict[str, JSONType]:
        """Convert to JSON format for storage."""
        return {
            "data": self.data,
            "metadata": self._get_metadata(),
        }

    def _get_metadata(self) -> dict[str, str]:
        """Get metadata for the data object."""
        return {
            "data_type": self.data_type,
            "timestamp": self.timestamp.isoformat(),
        }

    def get_storage_path(self) -> str:
        """Generate storage path."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"oanda/{self.data_type}/{date_str}.json.gz"


class InstrumentsData(BaseOANDAData):
    """Container for instruments list data."""

    data_type: str = "instruments-list"

    def get_instruments_list(self) -> list[str]:
        """Get the list of instruments from the data."""
        if not isinstance(self.data, dict):
            raise ValueError("Data is not a dictionary")
        instruments = self.data.get("instruments", [])
        return [inst.get("name", "") for inst in instruments if "name" in inst]


class CandlesData(BaseOANDAData):
    """Container for candles data."""

    instrument: str
    data_type: str = "candles"

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata["instrument"] = self.instrument
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"oanda/{self.data_type}/{date_str}/{self.instrument}.json.gz"
