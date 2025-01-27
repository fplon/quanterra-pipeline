import pytest

from src.ingest.data_sources.eodhd.factory import EODHDProcessorFactory, ProcessorType
from src.ingest.data_sources.eodhd.models import EODHDConfig
from src.ingest.data_sources.eodhd.processors import (
    EconomicEventDataProcessor,
    EODIngestionProcessor,
    ExchangeDataProcessor,
    ExchangeSymbolDataProcessor,
    InstrumentDataProcessor,
    MacroDataProcessor,
)


@pytest.fixture
def mock_config() -> EODHDConfig:
    """Create a mock EODHD configuration for testing."""
    return EODHDConfig(
        api_key="test_key",
        base_url="https://test.url",
        bucket_name="test-bucket",
        exchanges=["LSE", "NYSE"],
        instruments=["AAPL.US", "GOOGL.US"],
        macro_indicators=["GDP", "CPI"],
        macro_countries=["US", "UK"],
    )


class TestProcessorType:
    """Test suite for ProcessorType enum."""

    def test_processor_type_values(self) -> None:
        """Test that ProcessorType enum has correct values."""
        assert ProcessorType.EXCHANGE.value == "exchange"
        assert ProcessorType.EXCHANGE_SYMBOL.value == "exchange_symbol"
        assert ProcessorType.INSTRUMENT.value == "instrument"
        assert ProcessorType.MACRO.value == "macro-indicator"
        assert ProcessorType.ECONOMIC_EVENT.value == "economic_event"


class TestEODHDProcessorFactory:
    """Test suite for EODHDProcessorFactory."""

    def test_create_exchange_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of exchange processor."""
        processor = EODHDProcessorFactory.create_processor(ProcessorType.EXCHANGE, mock_config)
        assert isinstance(processor, ExchangeDataProcessor)
        assert processor.config == mock_config

    def test_create_exchange_symbol_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of exchange symbol processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.EXCHANGE_SYMBOL, mock_config
        )
        assert isinstance(processor, ExchangeSymbolDataProcessor)
        assert processor.config == mock_config

    def test_create_instrument_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of instrument processor."""
        processor = EODHDProcessorFactory.create_processor(ProcessorType.INSTRUMENT, mock_config)
        assert isinstance(processor, InstrumentDataProcessor)
        assert processor.config == mock_config

    def test_create_macro_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of macro processor."""
        processor = EODHDProcessorFactory.create_processor(ProcessorType.MACRO, mock_config)
        assert isinstance(processor, MacroDataProcessor)
        assert processor.config == mock_config

    def test_create_economic_event_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of economic event processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.ECONOMIC_EVENT, mock_config
        )
        assert isinstance(processor, EconomicEventDataProcessor)
        assert processor.config == mock_config

    def test_processor_inheritance(self, mock_config: EODHDConfig) -> None:
        """Test that all created processors inherit from EODIngestionProcessor."""
        for processor_type in ProcessorType:
            processor = EODHDProcessorFactory.create_processor(processor_type, mock_config)
            assert isinstance(processor, EODIngestionProcessor)

    @pytest.mark.parametrize(
        "processor_type",
        [
            ProcessorType.EXCHANGE,
            ProcessorType.EXCHANGE_SYMBOL,
            ProcessorType.INSTRUMENT,
            ProcessorType.MACRO,
            ProcessorType.ECONOMIC_EVENT,
        ],
    )
    def test_processor_config_assignment(
        self, processor_type: ProcessorType, mock_config: EODHDConfig
    ) -> None:
        """Test that config is correctly assigned for each processor type."""
        processor = EODHDProcessorFactory.create_processor(processor_type, mock_config)
        assert processor.config is mock_config
        assert processor.config.api_key == "test_key"
        assert processor.config.base_url == "https://test.url"
        assert processor.config.bucket_name == "test-bucket"
