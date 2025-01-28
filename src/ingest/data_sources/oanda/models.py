from dataclasses import dataclass
from datetime import datetime

from src.common.types import JSONType


@dataclass
class OANDAConfig:
    """Configuration for OANDA data ingestion."""

    api_key: str
    base_url: str
    bucket_name: str
    account_id: str
    instruments: list[str]
    granularity: str
    count: int
    price: str = "MBA"


@dataclass
class BaseOANDAData:
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


@dataclass
class InstrumentsData(BaseOANDAData):
    """Container for instruments list data."""

    def __init__(self, data: JSONType, timestamp: datetime):
        super().__init__(data=data, timestamp=timestamp, data_type="instruments-list")


@dataclass
class CandlesData(BaseOANDAData):
    """Container for candles data."""

    instrument: str

    def __init__(self, data: JSONType, instrument: str, timestamp: datetime):
        super().__init__(data=data, timestamp=timestamp, data_type="candles")
        self.instrument = instrument

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata["instrument"] = self.instrument
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"oanda/{self.data_type}/{date_str}/{self.instrument}.json.gz"


@dataclass
class StorageLocation:
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"
