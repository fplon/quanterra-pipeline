"""Unit tests for Yahoo Finance processor."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.common.types import JSONType
from src.ingest.core.context import PipelineContext
from src.ingest.data_sources.yahoo_finance.models import (
    YahooFinanceConfig,
    YahooFinanceData,
)
from src.ingest.data_sources.yahoo_finance.processors import YahooFinanceProcessor


@pytest.fixture
def mock_config() -> YahooFinanceConfig:
    """Create a mock config for testing."""
    return YahooFinanceConfig(
        bucket_name="test-bucket",
        tickers=["AAPL", "GOOGL", "MSFT"],
    )


@pytest.fixture
def mock_storage_client() -> MagicMock:
    """Create a mock storage client for testing."""
    return MagicMock(spec=GCPStorageClient)


@pytest.fixture
def mock_yf_client() -> MagicMock:
    """Create a mock Yahoo Finance client for testing."""
    return MagicMock()


@pytest.fixture
def pipeline_context() -> PipelineContext:
    """Create a pipeline context for testing."""
    return PipelineContext(pipeline_id="test_pipeline")


class TestYahooFinanceProcessor:
    """Test suite for Yahoo Finance processor."""

    @pytest.fixture
    def processor(
        self,
        mock_config: YahooFinanceConfig,
        mock_storage_client: MagicMock,
        mock_yf_client: MagicMock,
    ) -> YahooFinanceProcessor:
        """Create a processor instance for testing."""
        with patch("src.ingest.data_sources.yahoo_finance.processors.GCPStorageClient") as mock_gcp:
            with patch(
                "src.ingest.data_sources.yahoo_finance.processors.YahooFinanceClient"
            ) as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_yf_client
                return YahooFinanceProcessor(mock_config)

    def test_processor_initialisation(
        self, processor: YahooFinanceProcessor, mock_config: YahooFinanceConfig
    ) -> None:
        """Test processor initialisation."""
        assert processor.config == mock_config
        assert processor.name == "YahooFinanceProcessor"

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
        mock_storage_client: MagicMock,
        mock_config: YahooFinanceConfig,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test successful processing of Yahoo Finance data."""
        # Mock data
        mock_ticker_data: JSONType = {
            "AAPL": {"price": 150.0, "volume": 1000000},
            "GOOGL": {"price": 2500.0, "volume": 500000},
        }
        mock_market_data: JSONType = {
            "AAPL": {"market_cap": 2.5e12, "pe_ratio": 25.0},
            "GOOGL": {"market_cap": 1.8e12, "pe_ratio": 28.0},
        }

        # Configure mocks
        mock_yf_client.get_tickers_data.return_value = mock_ticker_data
        mock_yf_client.get_market_data.return_value = mock_market_data
        mock_yf_client.__enter__.return_value = mock_yf_client

        # Execute process
        result = await processor.process(pipeline_context)

        # Verify results
        assert len(result) == 2  # Two locations (ticker and market data)
        assert all(isinstance(loc, StorageLocation) for loc in result)

        # Verify client calls
        mock_yf_client.get_tickers_data.assert_called_once_with(mock_config.tickers)
        mock_yf_client.get_market_data.assert_called_once_with(mock_config.tickers)

        # Verify storage calls
        assert mock_storage_client.store_json_data.call_count == 2

        # Verify first storage call (ticker data)
        first_call_kwargs = mock_storage_client.store_json_data.call_args_list[0][1]
        assert first_call_kwargs["bucket_name"] == mock_config.bucket_name
        assert "tickers" in first_call_kwargs["blob_path"]
        assert first_call_kwargs["data"] == mock_ticker_data
        assert first_call_kwargs["compress"] is True

        # Verify second storage call (market data)
        second_call_kwargs = mock_storage_client.store_json_data.call_args_list[1][1]
        assert second_call_kwargs["bucket_name"] == mock_config.bucket_name
        assert "market" in second_call_kwargs["blob_path"]
        assert second_call_kwargs["data"] == mock_market_data
        assert second_call_kwargs["compress"] is True

    @pytest.mark.asyncio
    async def test_process_failure_ticker_data(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test failure handling when getting ticker data."""
        mock_yf_client.__enter__.return_value = mock_yf_client
        mock_yf_client.get_tickers_data.side_effect = Exception("API Error")

        with pytest.raises(Exception) as exc_info:
            await processor.process(pipeline_context)
        assert "API Error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_process_failure_market_data(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
        pipeline_context: PipelineContext,
    ) -> None:
        """Test failure handling when getting market data."""
        mock_yf_client.__enter__.return_value = mock_yf_client
        mock_yf_client.get_tickers_data.return_value = {"AAPL": {"price": 150.0}}
        mock_yf_client.get_market_data.side_effect = Exception("API Error")

        with pytest.raises(Exception) as exc_info:
            await processor.process(pipeline_context)
        assert "API Error" in str(exc_info.value)

    def test_store_data(
        self,
        processor: YahooFinanceProcessor,
        mock_storage_client: MagicMock,
    ) -> None:
        """Test data storage functionality."""
        test_data = YahooFinanceData(
            data={"test": "data"},
            timestamp=datetime.now(),
            data_type="test",
            ticker="BULK",
        )
        location = StorageLocation(bucket="test-bucket", path="test/path")

        processor._store_data(test_data, location)

        mock_storage_client.store_json_data.assert_called_once()
        call_kwargs = mock_storage_client.store_json_data.call_args[1]
        assert call_kwargs["bucket_name"] == location.bucket
        assert call_kwargs["blob_path"] == location.path
        assert call_kwargs["data"] == test_data.data
        assert call_kwargs["compress"] is True
