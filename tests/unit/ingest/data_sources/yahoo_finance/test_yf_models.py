"""Unit tests for Yahoo Finance models."""

from datetime import datetime

import pytest

from src.common.models import StorageLocation
from src.common.types import JSONType
from src.ingest.data_sources.yahoo_finance.models import (
    MarketData,
    TickerData,
    YahooFinanceConfig,
    YahooFinanceData,
)


@pytest.fixture
def sample_timestamp() -> datetime:
    """Create a sample timestamp for testing."""
    return datetime(2024, 1, 1, 12, 0)


class TestYahooFinanceConfig:
    """Test suite for YahooFinanceConfig model."""

    def test_valid_config(self) -> None:
        """Test creation of valid config."""
        config = YahooFinanceConfig(
            bucket_name="test-bucket",
            tickers=["AAPL", "GOOGL", "MSFT"],
        )
        assert config.bucket_name == "test-bucket"
        assert config.tickers == ["AAPL", "GOOGL", "MSFT"]

    def test_minimal_config(self) -> None:
        """Test creation of config with minimal fields (all are required)."""
        config = YahooFinanceConfig(
            bucket_name="test-bucket",
            tickers=["AAPL"],
        )
        assert config.bucket_name == "test-bucket"
        assert config.tickers == ["AAPL"]


class TestStorageLocation:
    """Test suite for StorageLocation."""

    def test_init(self) -> None:
        """Test initialisation of StorageLocation."""
        location = StorageLocation(bucket="test-bucket", path="test/path")
        assert location.bucket == "test-bucket"
        assert location.path == "test/path"


class TestYahooFinanceData:
    """Test suite for YahooFinanceData model."""

    class ConcreteData(YahooFinanceData):
        """Concrete implementation for testing abstract base class."""

        data_type: str = "test-data"

    def test_get_storage_path(self) -> None:
        """Test storage path generation."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = self.ConcreteData(
            data={"test": "data"},
            timestamp=timestamp,
            ticker="BULK",
        )

        path = data.get_storage_path()
        assert path == "yahoo_finance/2024/01/29/test-data.json.gz"


class TestTickerData:
    """Test suite for TickerData model."""

    def test_ticker_data_initialisation(self) -> None:
        """Test initialisation with ticker data."""
        test_data: JSONType = {
            "AAPL": {"price": 150.0, "volume": 1000000},
            "GOOGL": {"price": 2500.0, "volume": 500000},
        }
        timestamp = datetime.now()

        data = TickerData(
            data=test_data,
            timestamp=timestamp,
            data_type="tickers",
            ticker="BULK",
        )

        assert data.data == test_data
        assert data.timestamp == timestamp
        assert data.data_type == "tickers"

    def test_storage_path(self) -> None:
        """Test storage path generation for ticker data."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = TickerData(
            data={},
            timestamp=timestamp,
            data_type="tickers",
            ticker="BULK",
        )

        path = data.get_storage_path()
        assert path == "yahoo_finance/2024/01/29/tickers.json.gz"


class TestMarketData:
    """Test suite for MarketData model."""

    def test_market_data_initialisation(self) -> None:
        """Test initialisation with market data."""
        test_data: JSONType = {
            "AAPL": {"market_cap": 2.5e12, "pe_ratio": 25.0},
            "GOOGL": {"market_cap": 1.8e12, "pe_ratio": 28.0},
        }
        timestamp = datetime.now()

        data = MarketData(
            data=test_data,
            timestamp=timestamp,
            data_type="market",
            ticker="BULK",
        )

        assert data.data == test_data
        assert data.timestamp == timestamp
        assert data.data_type == "market"

    def test_storage_path(self) -> None:
        """Test storage path generation for market data."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = MarketData(
            data={},
            timestamp=timestamp,
            data_type="market",
            ticker="BULK",
        )

        path = data.get_storage_path()
        assert path == "yahoo_finance/2024/01/29/market.json.gz"
