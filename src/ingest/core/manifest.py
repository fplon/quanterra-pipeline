from enum import Enum
from typing import Any

from pydantic import BaseModel


class ProcessorType(str, Enum):
    # OANDA
    OANDA_INSTRUMENTS = "oanda_instruments"
    OANDA_CANDLES = "oanda_candles"
    # EODHD
    EODHD_EXCHANGE = "eodhd_exchange"
    EODHD_EXCHANGE_SYMBOL = "eodhd_exchange_symbol"
    EODHD_INSTRUMENT = "eodhd_instrument"
    EODHD_MACRO = "eodhd_macro"
    EODHD_ECONOMIC_EVENT = "eodhd_economic_event"
    # Yahoo Finance
    YF_MARKET = "yf_market"


class ProcessorManifest(BaseModel):
    type: ProcessorType
    config: dict[str, Any] = {}
    depends_on: list[str] | None = None


class PipelineManifest(BaseModel):
    name: str
    processors: list[ProcessorManifest]
    settings: dict[str, Any]
