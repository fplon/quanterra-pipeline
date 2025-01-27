"""Unit tests for Yahoo Finance processor."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.ingest.data_sources.yahoo_finance.client import YahooFinanceClient
from src.ingest.data_sources.yahoo_finance.models import (
    StorageLocation,
    YahooFinanceConfig,
    YahooFinanceData,
)
from src.ingest.data_sources.yahoo_finance.processors import YahooFinanceProcessor


@pytest.fixture
def mock_gcp_client() -> MagicMock:
    """Create a mock GCP storage client."""
    return MagicMock(spec=GCPStorageClient)


@pytest.fixture
def mock_yf_client() -> MagicMock:
    """Create a mock Yahoo Finance client."""
    mock = MagicMock(spec=YahooFinanceClient)
    mock.__enter__.return_value = mock
    return mock


@pytest.fixture
def config() -> YahooFinanceConfig:
    """Create a test configuration."""
    return YahooFinanceConfig(
        tickers=["AAPL", "MSFT"],
        bucket_name="test-bucket",
    )


@pytest.fixture
def processor(
    config: YahooFinanceConfig, mock_gcp_client: MagicMock, mock_yf_client: MagicMock
) -> YahooFinanceProcessor:
    """Create a YahooFinanceProcessor instance with mocked dependencies."""
    with (
        patch(
            "src.ingest.data_sources.yahoo_finance.processors.GCPStorageClient",
            return_value=mock_gcp_client,
        ),
        patch(
            "src.ingest.data_sources.yahoo_finance.processors.YahooFinanceClient",
            return_value=mock_yf_client,
        ),
    ):
        return YahooFinanceProcessor(config)


class TestYahooFinanceProcessor:
    """Test suite for YahooFinanceProcessor."""

    def test_init(self, processor: YahooFinanceProcessor, config: YahooFinanceConfig) -> None:
        """Test processor initialisation."""
        assert processor.config == config
        assert isinstance(processor.storage_client, GCPStorageClient)
        assert isinstance(processor.yf_client, YahooFinanceClient)

    def test_store_data(
        self,
        processor: YahooFinanceProcessor,
        mock_gcp_client: MagicMock,
    ) -> None:
        """Test storing data in GCP bucket."""
        # Setup test data
        data = YahooFinanceData(
            data={"test": "data"},
            timestamp=datetime.now(),
            data_type="test",
        )
        location = StorageLocation(bucket="test-bucket", path="test/path")

        # Execute
        processor._store_data(data, location)

        # Verify
        mock_gcp_client.store_json_data.assert_called_once_with(
            bucket_name=location.bucket,
            blob_path=location.path,
            data=data.data,  # Note: convert_to_json_safe is mocked implicitly
            compress=True,
        )

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
        mock_gcp_client: MagicMock,
        config: YahooFinanceConfig,
    ) -> None:
        """Test successful processing of Yahoo Finance data."""
        # Setup mock data
        ticker_data = {
            "info": {"AAPL": {"name": "Apple Inc"}},
            "financials": {"AAPL": {"balance_sheet": {}}},
        }
        market_data = {
            "AAPL": {
                "Open": {datetime(2024, 1, 1): 150.0},
                "Close": {datetime(2024, 1, 1): 155.0},
            }
        }

        mock_yf_client.get_tickers_data.return_value = ticker_data
        mock_yf_client.get_market_data.return_value = market_data

        # Execute
        locations = await processor.process()

        # Verify
        assert len(locations) == 2
        assert all(isinstance(loc, StorageLocation) for loc in locations)
        assert all(loc.bucket == config.bucket_name for loc in locations)

        mock_yf_client.get_tickers_data.assert_called_once_with(config.tickers)
        mock_yf_client.get_market_data.assert_called_once_with(config.tickers)
        assert mock_gcp_client.store_json_data.call_count == 2

    @pytest.mark.asyncio
    async def test_process_error(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
    ) -> None:
        """Test error handling during processing."""
        # Setup mock to raise an exception
        mock_yf_client.get_tickers_data.side_effect = Exception("Test error")

        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            await processor.process()

        assert str(exc_info.value) == "Test error"
        mock_yf_client.get_tickers_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_partial_failure(
        self,
        processor: YahooFinanceProcessor,
        mock_yf_client: MagicMock,
        config: YahooFinanceConfig,
    ) -> None:
        """Test handling of partial failure during processing."""
        # Setup: ticker data succeeds but market data fails
        ticker_data = {
            "info": {"AAPL": {"name": "Apple Inc"}},
            "financials": {"AAPL": {"balance_sheet": {}}},
        }
        mock_yf_client.get_tickers_data.return_value = ticker_data
        mock_yf_client.get_market_data.side_effect = Exception("Market data error")

        # Execute and verify
        with pytest.raises(Exception) as exc_info:
            await processor.process()

        assert str(exc_info.value) == "Market data error"
        mock_yf_client.get_tickers_data.assert_called_once_with(config.tickers)
        mock_yf_client.get_market_data.assert_called_once_with(config.tickers)
