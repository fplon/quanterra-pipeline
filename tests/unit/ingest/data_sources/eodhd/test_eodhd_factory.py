import pytest

from src.ingest.core.manifest import ProcessorType
from src.ingest.data_sources.eodhd.factory import EODHDProcessorFactory
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


class TestEODHDProcessorFactory:
    """Test suite for EODHDProcessorFactory."""

    def test_create_exchange_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of exchange processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.EODHD_EXCHANGE, mock_config
        )
        assert isinstance(processor, ExchangeDataProcessor)
        assert isinstance(processor, EODIngestionProcessor)
        assert processor.config == mock_config

    def test_create_exchange_symbol_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of exchange symbol processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.EODHD_EXCHANGE_SYMBOL, mock_config
        )
        assert isinstance(processor, ExchangeSymbolDataProcessor)
        assert isinstance(processor, EODIngestionProcessor)
        assert processor.config == mock_config

    def test_create_instrument_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of instrument processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.EODHD_INSTRUMENT, mock_config
        )
        assert isinstance(processor, InstrumentDataProcessor)
        assert isinstance(processor, EODIngestionProcessor)
        assert processor.config == mock_config

    def test_create_macro_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of macro processor."""
        processor = EODHDProcessorFactory.create_processor(ProcessorType.EODHD_MACRO, mock_config)
        assert isinstance(processor, MacroDataProcessor)
        assert isinstance(processor, EODIngestionProcessor)
        assert processor.config == mock_config

    def test_create_economic_event_processor(self, mock_config: EODHDConfig) -> None:
        """Test creation of economic event processor."""
        processor = EODHDProcessorFactory.create_processor(
            ProcessorType.EODHD_ECONOMIC_EVENT, mock_config
        )
        assert isinstance(processor, EconomicEventDataProcessor)
        assert isinstance(processor, EODIngestionProcessor)
        assert processor.config == mock_config

    def test_processor_inheritance(self, mock_config: EODHDConfig) -> None:
        """Test that all created processors inherit from EODIngestionProcessor."""
        eodhd_processor_types = [
            ProcessorType.EODHD_EXCHANGE,
            ProcessorType.EODHD_EXCHANGE_SYMBOL,
            ProcessorType.EODHD_INSTRUMENT,
            ProcessorType.EODHD_MACRO,
            ProcessorType.EODHD_ECONOMIC_EVENT,
        ]
        for processor_type in eodhd_processor_types:
            processor = EODHDProcessorFactory.create_processor(processor_type, mock_config)
            assert isinstance(processor, EODIngestionProcessor)

    @pytest.mark.parametrize(
        "processor_type",
        [
            ProcessorType.EODHD_EXCHANGE,
            ProcessorType.EODHD_EXCHANGE_SYMBOL,
            ProcessorType.EODHD_INSTRUMENT,
            ProcessorType.EODHD_MACRO,
            ProcessorType.EODHD_ECONOMIC_EVENT,
        ],
    )
    def test_processor_config_assignment(
        self, processor_type: ProcessorType, mock_config: EODHDConfig
    ) -> None:
        """Test that config is correctly assigned for each processor type."""
        processor = EODHDProcessorFactory.create_processor(processor_type, mock_config)
        assert isinstance(
            processor, EODIngestionProcessor
        )  # Ensure proper type before accessing config
        assert processor.config is mock_config
        assert processor.config.api_key == "test_key"
        assert processor.config.base_url == "https://test.url"
        assert processor.config.bucket_name == "test-bucket"

    def test_invalid_processor_type(self, mock_config: EODHDConfig) -> None:
        """Test that creating a processor with an invalid type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            EODHDProcessorFactory.create_processor(ProcessorType.OANDA_CANDLES, mock_config)
        assert "Unsupported processor type" in str(exc_info.value)

    def test_non_eodhd_processor_type(self, mock_config: EODHDConfig) -> None:
        """Test that creating a processor with a non-EODHD type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            EODHDProcessorFactory.create_processor(ProcessorType.YF_MARKET, mock_config)
        assert "Unsupported processor type" in str(exc_info.value)
