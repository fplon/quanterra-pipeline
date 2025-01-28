import pytest

from src.ingest.data_sources.oanda.factory import OANDAProcessorFactory, ProcessorType
from src.ingest.data_sources.oanda.models import OANDAConfig
from src.ingest.data_sources.oanda.processors import (
    CandlesProcessor,
    InstrumentsProcessor,
    OANDAIngestionProcessor,
)


@pytest.fixture
def mock_config() -> OANDAConfig:
    """Create a mock OANDA configuration for testing."""
    return OANDAConfig(
        api_key="test_key",
        base_url="https://test.url",
        bucket_name="test-bucket",
        account_id="test-account",
        instruments=["EUR_USD", "GBP_USD"],
        granularity="H1",
        count=1000,
    )


class TestProcessorType:
    """Test suite for ProcessorType enum."""

    def test_processor_type_values(self) -> None:
        """Test that ProcessorType enum has correct values."""
        assert ProcessorType.INSTRUMENTS.value == "instruments"
        assert ProcessorType.CANDLES.value == "candles"


class TestOANDAProcessorFactory:
    """Test suite for OANDAProcessorFactory."""

    def test_create_instruments_processor(self, mock_config: OANDAConfig) -> None:
        """Test creation of instruments processor."""
        processor = OANDAProcessorFactory.create_processor(ProcessorType.INSTRUMENTS, mock_config)
        assert isinstance(processor, InstrumentsProcessor)
        assert processor.config == mock_config

    def test_create_candles_processor(self, mock_config: OANDAConfig) -> None:
        """Test creation of candles processor."""
        processor = OANDAProcessorFactory.create_processor(ProcessorType.CANDLES, mock_config)
        assert isinstance(processor, CandlesProcessor)
        assert processor.config == mock_config

    def test_processor_inheritance(self, mock_config: OANDAConfig) -> None:
        """Test that all created processors inherit from OANDAIngestionProcessor."""
        for processor_type in ProcessorType:
            processor = OANDAProcessorFactory.create_processor(processor_type, mock_config)
            assert isinstance(processor, OANDAIngestionProcessor)

    @pytest.mark.parametrize(
        "processor_type",
        [
            ProcessorType.INSTRUMENTS,
            ProcessorType.CANDLES,
        ],
    )
    def test_processor_config_assignment(
        self, processor_type: ProcessorType, mock_config: OANDAConfig
    ) -> None:
        """Test that config is correctly assigned for each processor type."""
        processor = OANDAProcessorFactory.create_processor(processor_type, mock_config)
        assert processor.config is mock_config
        assert processor.config.api_key == "test_key"
        assert processor.config.base_url == "https://test.url"
        assert processor.config.bucket_name == "test-bucket"
        assert processor.config.account_id == "test-account"
        assert processor.config.instruments == ["EUR_USD", "GBP_USD"]
        assert processor.config.granularity == "H1"
        assert processor.config.count == 1000
