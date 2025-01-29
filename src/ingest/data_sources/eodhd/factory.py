from src.ingest.core.manifest import ProcessorType
from src.ingest.core.processor import BaseProcessor

from .models import EODHDConfig
from .processors import (
    EconomicEventDataProcessor,
    ExchangeDataProcessor,
    ExchangeSymbolDataProcessor,
    InstrumentDataProcessor,
    MacroDataProcessor,
)


class EODHDProcessorFactory:
    """Factory for creating EODHD ingestion processors."""

    @staticmethod
    def create_processor(
        processor_type: ProcessorType,
        config: EODHDConfig,
    ) -> BaseProcessor:
        if processor_type == ProcessorType.EODHD_EXCHANGE:
            return ExchangeDataProcessor(config)
        elif processor_type == ProcessorType.EODHD_EXCHANGE_SYMBOL:
            return ExchangeSymbolDataProcessor(config)
        elif processor_type == ProcessorType.EODHD_INSTRUMENT:
            return InstrumentDataProcessor(config)
        elif processor_type == ProcessorType.EODHD_MACRO:
            return MacroDataProcessor(config)
        elif processor_type == ProcessorType.EODHD_ECONOMIC_EVENT:
            return EconomicEventDataProcessor(config)
        raise ValueError(f"Unsupported processor type: {processor_type}")
