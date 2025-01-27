from dataclasses import dataclass
from datetime import datetime

from src.common.types import JSONType


@dataclass
class YahooFinanceConfig:
    """Configuration for Yahoo Finance data ingestion."""

    bucket_name: str
    tickers: list[str]


@dataclass
class StorageLocation:
    """Storage location for data."""

    bucket: str
    path: str


@dataclass
class YahooFinanceData:
    """Base class for Yahoo Finance data."""

    data: JSONType
    timestamp: datetime
    data_type: str

    def get_storage_path(self) -> str:
        """Get the storage path for the data."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"yahoo_finance/{date_str}/{self.data_type}.json"


@dataclass
class TickerData(YahooFinanceData):
    """Ticker data from Yahoo Finance."""

    pass


@dataclass
class MarketData(YahooFinanceData):
    """Market data from Yahoo Finance."""

    pass
