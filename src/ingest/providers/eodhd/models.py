from dataclasses import dataclass
from datetime import datetime

from src.common.types import JSONType


@dataclass
class EODHDConfig:
    """Configuration for EODHD provider."""

    api_key: str
    base_url: str
    bucket_name: str
    exchanges: list[str]
    instruments: list[str]


@dataclass
class BaseEODHDData:
    """Base class for all EODHD data types."""

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
        return f"eodhd/{self.data_type}/{date_str}.json.gz"


@dataclass
class ExchangeData(BaseEODHDData):
    """Container for exchange list data."""

    def __init__(self, data: JSONType, timestamp: datetime):
        super().__init__(data=data, timestamp=timestamp, data_type="exchanges-list")


@dataclass
class ExchangeSymbolData(BaseEODHDData):
    """Container for exchange symbol data."""

    exchange: str

    def __init__(self, data: JSONType, exchange: str, timestamp: datetime):
        super().__init__(data=data, timestamp=timestamp, data_type="exchange-symbol-list")
        self.exchange = exchange

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata["exchange"] = self.exchange
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/{self.data_type}/{date_str}/{self.exchange}.json.gz"


@dataclass
class InstrumentData(BaseEODHDData):
    """Container for instrument-specific data."""

    code: str
    exchange: str

    def __init__(
        self,
        data: JSONType,
        code: str,
        exchange: str,
        data_type: str,
        timestamp: datetime,
    ):
        super().__init__(data=data, timestamp=timestamp, data_type=data_type)
        self.code = code
        self.exchange = exchange

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata.update(
            {
                "code": self.code,
                "exchange": self.exchange,
            }
        )
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/{self.data_type}/{date_str}/{self.exchange}/{self.code}.json.gz"


@dataclass
class StorageLocation:
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"
