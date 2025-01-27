"""Unit tests for Yahoo Finance models."""

from datetime import datetime

import pytest

from src.common.types import JSONType
from src.ingest.data_sources.yahoo_finance.models import (
    MarketData,
    StorageLocation,
    TickerData,
    YahooFinanceConfig,
    YahooFinanceData,
)


@pytest.fixture
def sample_timestamp() -> datetime:
    """Create a sample timestamp for testing."""
    return datetime(2024, 1, 1, 12, 0)


class TestYahooFinanceConfig:
    """Test suite for YahooFinanceConfig."""

    def test_init(self) -> None:
        """Test initialization of YahooFinanceConfig."""
        config = YahooFinanceConfig(bucket_name="test-bucket", tickers=["AAPL", "MSFT"])
        assert config.bucket_name == "test-bucket"
        assert config.tickers == ["AAPL", "MSFT"]


class TestStorageLocation:
    """Test suite for StorageLocation."""

    def test_init(self) -> None:
        """Test initialization of StorageLocation."""
        location = StorageLocation(bucket="test-bucket", path="test/path")
        assert location.bucket == "test-bucket"
        assert location.path == "test/path"


class TestYahooFinanceData:
    """Test suite for YahooFinanceData."""

    @pytest.fixture
    def sample_data(self) -> JSONType:
        """Create sample data for testing."""
        return {"test": "data"}

    def test_init(self, sample_data: JSONType, sample_timestamp: datetime) -> None:
        """Test initialization of YahooFinanceData."""
        data = YahooFinanceData(
            data=sample_data,
            timestamp=sample_timestamp,
            data_type="test",
        )
        assert data.data == sample_data
        assert data.timestamp == sample_timestamp
        assert data.data_type == "test"

    def test_get_storage_path(self, sample_data: JSONType, sample_timestamp: datetime) -> None:
        """Test storage path generation."""
        data = YahooFinanceData(
            data=sample_data,
            timestamp=sample_timestamp,
            data_type="test",
        )
        expected_path = "yahoo_finance/2024/01/01/test.json"
        assert data.get_storage_path() == expected_path


class TestTickerData:
    """Test suite for TickerData."""

    @pytest.fixture
    def sample_ticker_data(self) -> JSONType:
        """Create sample ticker data for testing."""
        return {
            "info": {"AAPL": {"name": "Apple Inc"}},
            "financials": {"AAPL": {"balance_sheet": {}}},
        }

    def test_init(self, sample_ticker_data: JSONType, sample_timestamp: datetime) -> None:
        """Test initialization of TickerData."""
        data = TickerData(
            data=sample_ticker_data,
            timestamp=sample_timestamp,
            data_type="tickers",
        )
        assert data.data == sample_ticker_data
        assert data.timestamp == sample_timestamp
        assert data.data_type == "tickers"

    def test_inheritance(self, sample_ticker_data: JSONType, sample_timestamp: datetime) -> None:
        """Test that TickerData inherits correctly from YahooFinanceData."""
        data = TickerData(
            data=sample_ticker_data,
            timestamp=sample_timestamp,
            data_type="tickers",
        )
        assert isinstance(data, YahooFinanceData)
        assert data.get_storage_path() == "yahoo_finance/2024/01/01/tickers.json"


class TestMarketData:
    """Test suite for MarketData."""

    @pytest.fixture
    def sample_market_data(self) -> JSONType:
        """Create sample market data for testing."""
        timestamp = "2024-01-01T00:00:00"
        return {
            "AAPL": {
                "Open": {timestamp: 150.0},
                "Close": {timestamp: 155.0},
            }
        }

    def test_init(self, sample_market_data: JSONType, sample_timestamp: datetime) -> None:
        """Test initialization of MarketData."""
        data = MarketData(
            data=sample_market_data,
            timestamp=sample_timestamp,
            data_type="market",
        )
        assert data.data == sample_market_data
        assert data.timestamp == sample_timestamp
        assert data.data_type == "market"

    def test_inheritance(self, sample_market_data: JSONType, sample_timestamp: datetime) -> None:
        """Test that MarketData inherits correctly from YahooFinanceData."""
        data = MarketData(
            data=sample_market_data,
            timestamp=sample_timestamp,
            data_type="market",
        )
        assert isinstance(data, YahooFinanceData)
        assert data.get_storage_path() == "yahoo_finance/2024/01/01/market.json"
