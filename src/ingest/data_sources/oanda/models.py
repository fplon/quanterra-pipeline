from datetime import datetime

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel

from src.common.types import JSONType


class OANDAConfig(BaseModel):
    """Configuration for OANDA data ingestion."""

    api_key: str
    base_url: str
    bucket_name: str
    account_id: str
    granularity: str
    count: int
    price: str = "MBA"
    instruments: list[str] | None = None
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


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
