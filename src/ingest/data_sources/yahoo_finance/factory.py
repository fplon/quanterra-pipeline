from src.ingest.core.manifest import ProcessorType
from src.ingest.core.processor import BaseProcessor

from .models import YahooFinanceConfig
from .processors import YahooFinanceProcessor

# TODO I only need one factory


class YahooFinanceProcessorFactory:
    """Factory for creating Yahoo Finance ingestion processors."""

    @staticmethod
    def create_processor(
        processor_type: ProcessorType,
        config: YahooFinanceConfig,
    ) -> BaseProcessor:
        if processor_type == ProcessorType.YF_MARKET:
            return YahooFinanceProcessor(config)

        raise ValueError(f"Unsupported processor type: {processor_type}")
