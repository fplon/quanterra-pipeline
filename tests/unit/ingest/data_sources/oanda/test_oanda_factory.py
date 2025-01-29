from unittest.mock import AsyncMock, MagicMock

import pytest

from src.common.gcp.client import GCPStorageClient
from src.ingest.core.manifest import ProcessorType
from src.ingest.data_sources.oanda.factory import OANDAProcessorFactory
from src.ingest.data_sources.oanda.models import OANDAConfig
from src.ingest.data_sources.oanda.processors import (
    CandlesProcessor,
    InstrumentsProcessor,
    OANDAIngestionProcessor,
)


@pytest.fixture
def mock_config() -> OANDAConfig:
    """Create a mock config for testing."""
    return OANDAConfig(
        api_key="test_key",
        base_url="http://test.com",
        bucket_name="test-bucket",
        account_id="test-account",
        granularity="H1",
        count=100,
        price="MBA",
        instruments=["EUR_USD", "GBP_USD"],
    )


@pytest.fixture
def mock_storage_client() -> MagicMock:
    """Create a mock storage client for testing."""
    return MagicMock(spec=GCPStorageClient)


@pytest.fixture
def mock_oanda_client() -> AsyncMock:
    """Create a mock OANDA client for testing."""
    return AsyncMock()


class TestOANDAProcessorFactory:
    """Test suite for OANDA processor factory."""

    @pytest.fixture
    def factory(self) -> OANDAProcessorFactory:
        """Create a factory instance for testing."""
        return OANDAProcessorFactory()

    def test_create_instruments_processor(
        self,
        factory: OANDAProcessorFactory,
        mock_config: OANDAConfig,
    ) -> None:
        """Test creation of instruments processor."""
        processor = factory.create_processor(ProcessorType.OANDA_INSTRUMENTS, mock_config)
        assert isinstance(processor, InstrumentsProcessor)
        assert isinstance(processor, OANDAIngestionProcessor)
        assert processor.config == mock_config

    def test_create_candles_processor(
        self,
        factory: OANDAProcessorFactory,
        mock_config: OANDAConfig,
    ) -> None:
        """Test creation of candles processor."""
        processor = factory.create_processor(ProcessorType.OANDA_CANDLES, mock_config)
        assert isinstance(processor, CandlesProcessor)
        assert isinstance(processor, OANDAIngestionProcessor)
        assert processor.config == mock_config

    def test_processor_inheritance(
        self,
        factory: OANDAProcessorFactory,
        mock_config: OANDAConfig,
    ) -> None:
        """Test that all created processors inherit from OANDAIngestionProcessor."""
        oanda_processor_types = [
            ProcessorType.OANDA_INSTRUMENTS,
            ProcessorType.OANDA_CANDLES,
        ]
        for processor_type in oanda_processor_types:
            processor = factory.create_processor(processor_type, mock_config)
            assert isinstance(processor, OANDAIngestionProcessor)

    @pytest.mark.parametrize(
        "processor_type",
        [
            ProcessorType.OANDA_INSTRUMENTS,
            ProcessorType.OANDA_CANDLES,
        ],
    )
    def test_processor_config_assignment(
        self,
        factory: OANDAProcessorFactory,
        processor_type: ProcessorType,
        mock_config: OANDAConfig,
    ) -> None:
        """Test that config is correctly assigned for each processor type."""
        processor = factory.create_processor(processor_type, mock_config)
        assert isinstance(processor, OANDAIngestionProcessor)
        assert processor.config == mock_config
        assert processor.config.api_key == "test_key"
        assert processor.config.base_url == "http://test.com"
        assert processor.config.bucket_name == "test-bucket"
        assert processor.config.account_id == "test-account"
        assert processor.config.granularity == "H1"
        assert processor.config.count == 100
        assert processor.config.price == "MBA"

    def test_invalid_processor_type(
        self,
        factory: OANDAProcessorFactory,
        mock_config: OANDAConfig,
    ) -> None:
        """Test that creating a processor with an invalid type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            factory.create_processor(ProcessorType.EODHD_EXCHANGE, mock_config)
        assert "Unsupported processor type" in str(exc_info.value)

    def test_non_oanda_processor_type(
        self,
        factory: OANDAProcessorFactory,
        mock_config: OANDAConfig,
    ) -> None:
        """Test that creating a processor with a non-OANDA type raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            factory.create_processor(ProcessorType.YF_MARKET, mock_config)
        assert "Unsupported processor type" in str(exc_info.value)
