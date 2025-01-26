from enum import Enum

from .models import EODHDConfig
from .processors import (
    EconomicEventDataProcessor,
    EODIngestionProcessor,
    ExchangeDataProcessor,
    ExchangeSymbolDataProcessor,
    InstrumentDataProcessor,
    MacroDataProcessor,
)


class ProcessorType(Enum):
    EXCHANGE = "exchange"
    EXCHANGE_SYMBOL = "exchange_symbol"
    INSTRUMENT = "instrument"
    MACRO = "macro-indicator"
    ECONOMIC_EVENT = "economic_event"


class EODHDProcessorFactory:
    """Factory for creating EODHD ingestion processors."""

    @staticmethod
    def create_processor(
        processor_type: ProcessorType,
        config: EODHDConfig,
    ) -> EODIngestionProcessor:
        if processor_type == ProcessorType.EXCHANGE:
            return ExchangeDataProcessor(config)
        elif processor_type == ProcessorType.EXCHANGE_SYMBOL:
            return ExchangeSymbolDataProcessor(config)
        elif processor_type == ProcessorType.INSTRUMENT:
            return InstrumentDataProcessor(config)
        elif processor_type == ProcessorType.MACRO:
            return MacroDataProcessor(config)
        elif processor_type == ProcessorType.ECONOMIC_EVENT:
            return EconomicEventDataProcessor(config)
