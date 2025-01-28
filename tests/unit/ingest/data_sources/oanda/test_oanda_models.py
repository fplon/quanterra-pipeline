from datetime import datetime
from typing import Any, cast

import pytest

from src.common.types import JSONType
from src.ingest.data_sources.oanda.models import (
    BaseOANDAData,
    CandlesData,
    InstrumentsData,
    OANDAConfig,
    StorageLocation,
)


@pytest.fixture
def sample_json_data() -> JSONType:
    return {"key": "value", "nested": {"data": 123}}


@pytest.fixture
def sample_timestamp() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0)


class TestOANDAConfig:
    """Test suite for OANDAConfig."""

    def test_config_initialisation(self) -> None:
        """Test that OANDAConfig initialises correctly with all parameters."""
        config = OANDAConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            account_id="test-account",
            instruments=["EUR_USD", "GBP_USD"],
            granularity="H1",
            count=1000,
        )

        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.account_id == "test-account"
        assert config.instruments == ["EUR_USD", "GBP_USD"]
        assert config.granularity == "H1"
        assert config.count == 1000
        assert config.price == "MBA"  # Default value

    def test_config_with_custom_price(self) -> None:
        """Test that OANDAConfig accepts custom price parameter."""
        config = OANDAConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            account_id="test-account",
            instruments=["EUR_USD"],
            granularity="H1",
            count=1000,
            price="B",
        )

        assert config.price == "B"


class TestBaseOANDAData:
    """Test suite for BaseOANDAData."""

    class ConcreteOANDAData(BaseOANDAData):
        """Concrete implementation for testing abstract base class."""

        def __init__(self, data: JSONType, timestamp: datetime):
            super().__init__(data=data, timestamp=timestamp, data_type="test-type")

    def test_to_json(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        """Test JSON conversion of base data."""
        base_data = self.ConcreteOANDAData(data=sample_json_data, timestamp=sample_timestamp)

        json_output = cast(dict[str, Any], base_data.to_json())
        assert json_output["data"] == sample_json_data
        assert json_output["metadata"]["data_type"] == "test-type"
        assert json_output["metadata"]["timestamp"] == "2024-01-01T12:00:00"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        """Test storage path generation for base data."""
        base_data = self.ConcreteOANDAData(data=sample_json_data, timestamp=sample_timestamp)

        expected_path = "oanda/test-type/2024/01/01.json.gz"
        assert base_data.get_storage_path() == expected_path


class TestInstrumentsData:
    """Test suite for InstrumentsData."""

    def test_instruments_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        """Test that InstrumentsData initialises correctly."""
        instruments_data = InstrumentsData(data=sample_json_data, timestamp=sample_timestamp)

        assert instruments_data.data == sample_json_data
        assert instruments_data.timestamp == sample_timestamp
        assert instruments_data.data_type == "instruments-list"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        """Test storage path generation for instruments data."""
        instruments_data = InstrumentsData(data=sample_json_data, timestamp=sample_timestamp)

        expected_path = "oanda/instruments-list/2024/01/01.json.gz"
        assert instruments_data.get_storage_path() == expected_path


class TestCandlesData:
    """Test suite for CandlesData."""

    def test_candles_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        """Test that CandlesData initialises correctly."""
        candles_data = CandlesData(
            data=sample_json_data, instrument="EUR_USD", timestamp=sample_timestamp
        )

        assert candles_data.data == sample_json_data
        assert candles_data.instrument == "EUR_USD"
        assert candles_data.timestamp == sample_timestamp
        assert candles_data.data_type == "candles"

    def test_get_metadata(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        """Test metadata generation for candles data."""
        candles_data = CandlesData(
            data=sample_json_data, instrument="EUR_USD", timestamp=sample_timestamp
        )

        metadata = candles_data._get_metadata()
        assert metadata["data_type"] == "candles"
        assert metadata["timestamp"] == "2024-01-01T12:00:00"
        assert metadata["instrument"] == "EUR_USD"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        """Test storage path generation for candles data."""
        candles_data = CandlesData(
            data=sample_json_data, instrument="EUR_USD", timestamp=sample_timestamp
        )

        expected_path = "oanda/candles/2024/01/01/EUR_USD.json.gz"
        assert candles_data.get_storage_path() == expected_path


class TestStorageLocation:
    """Test suite for StorageLocation."""

    def test_storage_location_string_representation(self) -> None:
        """Test string representation of storage location."""
        location = StorageLocation(bucket="test-bucket", path="test/path.json")
        assert str(location) == "test-bucket/test/path.json"
