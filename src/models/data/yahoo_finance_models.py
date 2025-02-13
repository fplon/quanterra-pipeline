from datetime import datetime

from pydantic import BaseModel

from src.models.data.json_objects import JSONType  # TODO better implementation
from src.utils.utillities import convert_to_json_safe


class YahooFinanceData(BaseModel):
    """Base class for Yahoo Finance data."""

    data: JSONType
    timestamp: datetime
    data_type: str
    ticker: str

    def get_storage_path(self) -> str:
        """Get the storage path for the data."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"yahoo_finance/{self.data_type}/{date_str}/{self.ticker}.json.gz"

    def _get_metadata(self) -> dict[str, str]:
        """Get the metadata for the data."""
        return {
            "data_type": self.data_type,
            "timestamp": self.timestamp.isoformat(),
        }

    def to_json(self) -> dict[str, JSONType]:
        """Convert to JSON format for storage."""
        return {
            "data": convert_to_json_safe(self.data),
            "metadata": self._get_metadata(),
        }


class TickerData(YahooFinanceData):
    """Ticker data from Yahoo Finance."""

    pass


class MarketData(YahooFinanceData):
    """Market data from Yahoo Finance."""

    pass
