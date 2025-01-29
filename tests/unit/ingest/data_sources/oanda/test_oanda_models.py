"""Unit tests for OANDA models."""

from datetime import datetime
from typing import Any, TypedDict, cast

import pytest
from pydantic import ValidationError

from src.common.types import JSONType
from src.ingest.data_sources.oanda.models import (
    BaseOANDAData,
    CandlesData,
    InstrumentsData,
    OANDAConfig,
)


class InstrumentRecord(TypedDict):
    """Type for instrument record."""

    name: str
    type: str


class TestOANDAConfig:
    """Test suite for OANDAConfig model."""

    def test_valid_config(self) -> None:
        """Test creation of valid config."""
        config = OANDAConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            account_id="test-account",
            granularity="H1",
            count=100,
            price="MBA",
            instruments=["EUR_USD", "GBP_USD"],
        )
        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.account_id == "test-account"
        assert config.granularity == "H1"
        assert config.count == 100
        assert config.price == "MBA"
        assert config.instruments == ["EUR_USD", "GBP_USD"]

    def test_minimal_config(self) -> None:
        """Test creation of config with only required fields."""
        config = OANDAConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            account_id="test-account",
            granularity="H1",
            count=100,
        )
        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.account_id == "test-account"
        assert config.granularity == "H1"
        assert config.count == 100
        assert config.price == "MBA"  # Default value
        assert config.instruments is None

    @pytest.mark.skip("TODO: Add test for invalid config")
    def test_invalid_config(self) -> None:
        """Test validation of invalid config."""
        with pytest.raises(ValidationError):
            OANDAConfig(
                api_key="test_key",
                base_url="not_a_url",  # Invalid URL format
                bucket_name="test-bucket",
                account_id="test-account",
                granularity="H1",
                count=100,
            )

        with pytest.raises(ValidationError):
            OANDAConfig(
                api_key="test_key",
                base_url="http://test.com",
                bucket_name="test-bucket",
                account_id="test-account",
                granularity="H1",
                count=-1,  # Invalid count
            )


class TestBaseOANDAData:
    """Test suite for BaseOANDAData model."""

    class ConcreteData(BaseOANDAData):
        """Concrete implementation for testing abstract base class."""

        data_type: str = "test-data"

    def test_to_json(self) -> None:
        """Test JSON conversion."""
        test_data: JSONType = {"key": "value"}
        timestamp = datetime.now()
        data = self.ConcreteData(data=test_data, timestamp=timestamp)

        json_data = cast(dict[str, Any], data.to_json())
        assert json_data["data"] == test_data
        assert json_data["metadata"]["data_type"] == "test-data"
        assert json_data["metadata"]["timestamp"] == timestamp.isoformat()

    def test_get_storage_path(self) -> None:
        """Test storage path generation."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = self.ConcreteData(data={}, timestamp=timestamp)

        path = data.get_storage_path()
        assert path == "oanda/test-data/2024/01/29.json.gz"


class TestInstrumentsData:
    """Test suite for InstrumentsData model."""

    def test_get_instruments_list(self) -> None:
        """Test extraction of instruments list."""
        test_data: dict[str, list[InstrumentRecord]] = {
            "instruments": [
                {"name": "EUR_USD", "type": "CURRENCY"},
                {"name": "GBP_USD", "type": "CURRENCY"},
            ]
        }
        data = InstrumentsData(data=cast(JSONType, test_data), timestamp=datetime.now())

        instruments = data.get_instruments_list()
        assert instruments == ["EUR_USD", "GBP_USD"]

    def test_get_instruments_list_invalid_data(self) -> None:
        """Test handling of invalid data format."""
        data = InstrumentsData(data=[], timestamp=datetime.now())  # Not a dict

        with pytest.raises(ValueError, match="Data is not a dictionary"):
            data.get_instruments_list()

    def test_get_instruments_list_empty_data(self) -> None:
        """Test handling of empty data."""
        data = InstrumentsData(data={"instruments": []}, timestamp=datetime.now())

        instruments = data.get_instruments_list()
        assert instruments == []

    def test_get_instruments_list_missing_name(self) -> None:
        """Test handling of instruments without name field."""
        test_data: dict[str, list[dict[str, str]]] = {
            "instruments": [
                {"type": "CURRENCY"},  # Missing name
                {"name": "GBP_USD", "type": "CURRENCY"},
            ]
        }
        data = InstrumentsData(data=cast(JSONType, test_data), timestamp=datetime.now())

        instruments = data.get_instruments_list()
        assert instruments == ["GBP_USD"]


class TestCandlesData:
    """Test suite for CandlesData model."""

    def test_metadata(self) -> None:
        """Test metadata includes instrument details."""
        data = CandlesData(
            data={},
            timestamp=datetime.now(),
            instrument="EUR_USD",
        )

        metadata = data._get_metadata()
        assert metadata["data_type"] == "candles"
        assert metadata["instrument"] == "EUR_USD"

    def test_storage_path(self) -> None:
        """Test storage path includes instrument details."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = CandlesData(
            data={},
            timestamp=timestamp,
            instrument="EUR_USD",
        )

        path = data.get_storage_path()
        assert path == "oanda/candles/2024/01/29/EUR_USD.json.gz"
