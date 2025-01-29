"""Unit tests for OANDA processors."""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.common.types import JSONType
from src.ingest.core.context import PipelineContext
from src.ingest.data_sources.oanda.models import (
    InstrumentsData,
    OANDAConfig,
)
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


@pytest.fixture
def pipeline_context() -> PipelineContext:
    """Create a pipeline context for testing."""
    return PipelineContext(pipeline_id="test_pipeline")


class TestInstrumentsProcessor:
    """Test suite for InstrumentsProcessor."""

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock, mock_oanda_client: AsyncMock
    ) -> InstrumentsProcessor:
        """Create a processor instance for testing."""
        with patch("src.ingest.data_sources.oanda.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.oanda.processors.OANDAClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_oanda_client
                return InstrumentsProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: InstrumentsProcessor,
        mock_oanda_client: AsyncMock,
        mock_storage_client: MagicMock,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test successful processing of instruments data."""
        # Mock data
        mock_data: JSONType = {
            "instruments": [
                {"name": "EUR_USD", "type": "CURRENCY"},
                {"name": "GBP_USD", "type": "CURRENCY"},
            ]
        }
        mock_oanda_client.get_instruments.return_value = mock_data

        # Execute process
        result = await processor.process(pipeline_context)

        # Test basic functionality
        assert len(result) == 1
        assert isinstance(result[0], StorageLocation)
        mock_oanda_client.get_instruments.assert_called_once()
        mock_storage_client.store_json_data.assert_called_once()

        # Test stored data
        stored_data = mock_storage_client.store_json_data.call_args[1]["data"]
        assert stored_data["data"] == mock_data
        assert "metadata" in stored_data

        # Test context shared state
        assert "available_instruments" in pipeline_context.shared_state
        assert isinstance(pipeline_context.shared_state["available_instruments"], list)
        assert len(pipeline_context.shared_state["available_instruments"]) == 2
        assert "EUR_USD" in pipeline_context.shared_state["available_instruments"]
        assert "GBP_USD" in pipeline_context.shared_state["available_instruments"]

    @pytest.mark.asyncio
    async def test_process_failure(
        self,
        processor: InstrumentsProcessor,
        mock_oanda_client: AsyncMock,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test failure handling when getting instruments data."""
        mock_oanda_client.get_instruments.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process(pipeline_context)


class TestCandlesProcessor:
    """Test suite for CandlesProcessor."""

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock, mock_oanda_client: AsyncMock
    ) -> CandlesProcessor:
        """Create a processor instance for testing."""
        with patch("src.ingest.data_sources.oanda.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.oanda.processors.OANDAClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_oanda_client
                return CandlesProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: CandlesProcessor,
        mock_oanda_client: AsyncMock,
        mock_storage_client: MagicMock,
        mock_config: OANDAConfig,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test successful processing of candles data."""
        # Mock data
        mock_data: JSONType = {
            "candles": [
                {
                    "time": "2024-01-29T00:00:00Z",
                    "bid": {"o": "1.0850", "h": "1.0855", "l": "1.0845", "c": "1.0852"},
                }
            ]
        }
        mock_oanda_client.get_candles.return_value = mock_data

        # Test with instruments from config
        result = await processor.process(pipeline_context)
        assert len(result) == len(mock_config.instruments)  # type: ignore
        assert all(isinstance(loc, StorageLocation) for loc in result)
        assert mock_oanda_client.get_candles.call_count == len(mock_config.instruments)  # type: ignore
        assert mock_storage_client.store_json_data.call_count == len(mock_config.instruments)  # type: ignore

        # Test with instruments from context
        pipeline_context.shared_state["available_instruments"] = ["USD_JPY"]
        mock_config.instruments = None  # Clear config instruments to force using context
        result = await processor.process(pipeline_context)
        assert len(result) == 1  # One instrument from context

        # Verify candles parameters
        call_kwargs = mock_oanda_client.get_candles.call_args[1]
        assert call_kwargs["granularity"] == mock_config.granularity
        assert call_kwargs["count"] == mock_config.count
        assert call_kwargs["price"] == mock_config.price

    @pytest.mark.asyncio
    async def test_process_failure(
        self,
        processor: CandlesProcessor,
        mock_oanda_client: AsyncMock,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test failure handling when getting candles data."""
        mock_oanda_client.get_candles.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process(pipeline_context)


class TestOANDAIngestionProcessor:
    """Test suite for the base OANDA ingestion processor."""

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock
    ) -> OANDAIngestionProcessor:
        """Create a processor instance for testing."""

        class TestProcessor(OANDAIngestionProcessor):
            async def process(self, context: PipelineContext) -> list[StorageLocation]:
                return []

        with patch("src.ingest.data_sources.oanda.processors.GCPStorageClient") as mock_gcp:
            mock_gcp.return_value = mock_storage_client
            return TestProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_store_data_success(
        self,
        processor: OANDAIngestionProcessor,
        mock_storage_client: MagicMock,
    ) -> None:
        """Test data storage functionality."""
        data = InstrumentsData(data={"test": "data"}, timestamp=datetime.now())
        location = StorageLocation(bucket="test-bucket", path="test/path")

        await processor._store_data(data, location)

        mock_storage_client.store_json_data.assert_called_once()
        call_kwargs = mock_storage_client.store_json_data.call_args[1]
        assert call_kwargs["bucket_name"] == location.bucket
        assert call_kwargs["blob_path"] == location.path
        assert call_kwargs["compress"] is True
