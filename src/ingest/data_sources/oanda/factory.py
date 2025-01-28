from enum import Enum

from .models import OANDAConfig
from .processors import CandlesProcessor, InstrumentsProcessor, OANDAIngestionProcessor


class ProcessorType(Enum):
    """Available OANDA processor types."""

    INSTRUMENTS = "instruments"
    CANDLES = "candles"


class OANDAProcessorFactory:
    """Factory for creating OANDA ingestion processors."""

    @staticmethod
    def create_processor(
        processor_type: ProcessorType,
        config: OANDAConfig,
    ) -> OANDAIngestionProcessor:
        """Create an OANDA processor instance based on the specified type."""
        if processor_type == ProcessorType.INSTRUMENTS:
            return InstrumentsProcessor(config)
        elif processor_type == ProcessorType.CANDLES:
            return CandlesProcessor(config)
