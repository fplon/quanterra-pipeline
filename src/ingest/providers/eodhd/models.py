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
class EODHDData:
    """Container for EODHD data."""

    code: str
    exchange: str
    data: JSONType
    timestamp: datetime
    data_type: str = "eod"  # eod, fundamental, etc.


@dataclass
class StorageLocation:
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"
