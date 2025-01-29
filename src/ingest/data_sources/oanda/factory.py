from src.ingest.core.factory import ProcessorFactory
from src.ingest.core.manifest import ProcessorType

from .models import OANDAConfig
from .processors import CandlesProcessor, InstrumentsProcessor, OANDAIngestionProcessor


class OANDAProcessorFactory(ProcessorFactory):
    """Factory for creating OANDA ingestion processors."""

    def create_processor(
        self,
        processor_type: ProcessorType,
        config: OANDAConfig,
    ) -> OANDAIngestionProcessor:
        """Create an OANDA processor instance based on the specified type."""
        if processor_type == ProcessorType.OANDA_INSTRUMENTS:
            return InstrumentsProcessor(config)
        elif processor_type == ProcessorType.OANDA_CANDLES:
            return CandlesProcessor(config)
        raise ValueError(f"Unsupported processor type: {processor_type}")
