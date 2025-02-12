from datetime import datetime

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel, Field

from src.common.types import JSONType


class EODHDConfig(BaseModel):
    """Configuration for EODHD provider."""

    api_key: str
    base_url: str
    bucket_name: str
    exchanges: list[str] = Field(default_factory=list)
    instruments: list[str] = Field(default_factory=list)
    macro_indicators: list[str] = Field(default_factory=list)
    macro_countries: list[str] = Field(default_factory=list)
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class BaseEODHDData(BaseModel):
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


class ExchangeData(BaseEODHDData):
    """Container for exchange list data."""

    data_type: str = "exchanges-list"

    def get_exchanges_list(self) -> list[str]:
        """Get the list of exchanges from the data."""
        if not isinstance(self.data, list):
            raise ValueError("Data is not a list of dictionaries (records)")
        return [ex.get("Code", "") for ex in self.data if "Code" in ex]


class BaseExchangeLevelData(BaseEODHDData):
    """Base class container for exchange-level data."""

    exchange: str
    data_type: str = "exchange-level"

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata["exchange"] = self.exchange
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/{self.data_type}/{date_str}/{self.exchange}.json.gz"


class ExchangeSymbolData(BaseExchangeLevelData):
    """Container for exchange symbol data."""

    def get_exchange_symbols_list(self) -> list[str]:
        """Get the list of exchange symbols from the data"""
        if not isinstance(self.data, list):
            raise ValueError("Data is not a list of dictionaries (records)")
        return [f"{ex.get('Code', '')}.{self.exchange}" for ex in self.data if "Code" in ex]


class ExchangeBulkData(BaseExchangeLevelData):
    """Container for exchange bulk data."""

    pass


class InstrumentData(BaseEODHDData):
    """Container for instrument-specific data."""

    code: str
    exchange: str

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


class MacroData(BaseEODHDData):
    """Container for macroeconomic data."""

    iso_code: str
    indicator: str
    data_type: str = "macro-indicators"

    def _get_metadata(self) -> dict[str, str]:
        metadata = super()._get_metadata()
        metadata.update(
            {
                "iso_code": self.iso_code,
                "indicator": self.indicator,
            }
        )
        return metadata

    def get_storage_path(self) -> str:
        date_str = self.timestamp.strftime("%Y/%m/%d")
        return f"eodhd/{self.data_type}/{date_str}/{self.iso_code}/{self.indicator}.json.gz"


class EconomicEventData(BaseEODHDData):
    """Container for economic event data."""

    data_type: str = "economic-events"
