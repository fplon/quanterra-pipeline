from dataclasses import dataclass
from typing import Optional


@dataclass
class BaseRequestConfig:
    """Base configuration for EODHD API requests."""

    endpoint: str


@dataclass
class ExchangeListConfig(BaseRequestConfig):
    """Configuration for exchange list endpoint."""

    endpoint: str = "exchanges-list"


@dataclass
class ExchangeSymbolConfig(BaseRequestConfig):
    """Configuration for exchange symbol list endpoint."""

    exchange: str
    asset_type: Optional[str]
    delisted: bool

    def __post_init__(self) -> None:
        self.endpoint = f"exchange-symbol-list/{self.exchange}"


@dataclass
class EODDataConfig(BaseRequestConfig):
    """Configuration for EOD data endpoint."""

    instrument: str
    exchange: str
    start_date: Optional[str]
    end_date: Optional[str]

    def __post_init__(self) -> None:
        self.endpoint = f"eod/{self.instrument}.{self.exchange}"


@dataclass
class FundamentalsConfig(BaseRequestConfig):
    """Configuration for fundamentals endpoint."""

    instrument: str
    exchange: str

    def __post_init__(self) -> None:
        self.endpoint = f"fundamentals/{self.instrument}.{self.exchange}"


@dataclass
class DividendsConfig(BaseRequestConfig):
    """Configuration for dividends endpoint."""

    instrument: str
    exchange: str

    def __post_init__(self) -> None:
        self.endpoint = f"div/{self.instrument}.{self.exchange}"


@dataclass
class SplitsConfig(BaseRequestConfig):
    """Configuration for splits endpoint."""

    instrument: str
    exchange: str

    def __post_init__(self) -> None:
        self.endpoint = f"splits/{self.instrument}.{self.exchange}"


@dataclass
class BulkEODConfig(BaseRequestConfig):
    """Configuration for bulk EOD endpoint."""

    exchange: str

    def __post_init__(self) -> None:
        self.endpoint = f"eod-bulk-last-day/{self.exchange}"


@dataclass
class EconomicEventsConfig(BaseRequestConfig):
    """Configuration for economic events endpoint."""

    country: str
    comparison: str
    start_date: str
    end_date: str
    limit: int

    def __post_init__(self) -> None:
        self.endpoint = "economic-events"


@dataclass
class MacroIndicatorConfig(BaseRequestConfig):
    """Configuration for macro indicator endpoint."""

    iso_code: str
    indicator: str

    def __post_init__(self) -> None:
        self.endpoint = f"macro-indicator/{self.iso_code}"


@dataclass
class NewsConfig(BaseRequestConfig):
    """Configuration for news endpoint."""

    instrument: str
    exchange: str

    def __post_init__(self) -> None:
        self.endpoint = "news"
