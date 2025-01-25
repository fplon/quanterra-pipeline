from dataclasses import dataclass
from datetime import datetime

from .types import JSONType


@dataclass
class EODHDConfig:
    """Configuration for EODHD provider."""

    api_key: str
    base_url: str
    bucket_name: str
    exchanges: list[str]
    instruments: list[str]


@dataclass
class EODHDExchangeData:
    """Container for EODHD exchange data."""

    data: JSONType
    timestamp: datetime

    def get_storage_path(self) -> str:
        """Generate storage path based on data type."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/exchanges-list/{date_str}.json.gz"


@dataclass
class EODHDExchangeSymbolData:
    """Container for EODHD exchange symbol data."""

    exchange: str
    data: JSONType
    timestamp: datetime

    def get_storage_path(self) -> str:
        """Generate storage path based on data type."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/exchange-symbol-list/{date_str}/{self.exchange}.json.gz"


@dataclass
class EODHDData:
    """Container for EODHD data."""

    code: str
    exchange: str
    data: JSONType
    timestamp: datetime
    data_type: str  # eod, dividends, splits

    def get_storage_path(self) -> str:
        """Generate storage path based on data type."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/{self.data_type}/{date_str}/{self.exchange}/{self.code}.json.gz"


@dataclass
class StorageLocation:
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"
