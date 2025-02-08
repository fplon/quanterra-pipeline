from datetime import datetime

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel

from src.common.types import JSONType


class YahooFinanceConfig(BaseModel):
    """Configuration for Yahoo Finance data ingestion."""

    bucket_name: str
    tickers: list[str]
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class YahooFinanceData(BaseModel):
    """Base class for Yahoo Finance data."""

    data: JSONType
    timestamp: datetime
    data_type: str

    def get_storage_path(self) -> str:
        """Get the storage path for the data."""
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"yahoo_finance/{date_str}/{self.data_type}.json.gz"


class TickerData(YahooFinanceData):
    """Ticker data from Yahoo Finance."""

    pass


class MarketData(YahooFinanceData):
    """Market data from Yahoo Finance."""

    pass
