"""Unit tests for EODHD models."""

from datetime import datetime
from typing import Any, TypedDict, cast

import pytest
from pydantic import ValidationError

from src.models.config.processor_settings import EODHDConfig
from src.models.data.eodhd_models import (
    BaseEODHDData,
    EconomicEventData,
    ExchangeData,
    ExchangeSymbolData,
    InstrumentData,
    MacroData,
)
from src.models.data.json_objects import JSONType


class ExchangeRecord(TypedDict):
    """Type for exchange record."""

    Code: str
    Name: str


class SymbolRecord(TypedDict):
    """Type for symbol record."""

    Code: str
    Name: str


class TestEODHDConfig:
    """Test suite for EODHDConfig model."""

    def test_valid_config(self) -> None:
        """Test creation of valid config."""
        config = EODHDConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            exchanges=["LSE", "NYSE"],
            instruments=["AAPL.US", "GOOGL.US"],
            macro_indicators=["GDP", "CPI"],
            macro_countries=["US", "UK"],
        )
        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.exchanges == ["LSE", "NYSE"]
        assert config.instruments == ["AAPL.US", "GOOGL.US"]
        assert config.macro_indicators == ["GDP", "CPI"]
        assert config.macro_countries == ["US", "UK"]

    def test_minimal_config(self) -> None:
        """Test creation of config with only required fields."""
        config = EODHDConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
        )
        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.exchanges == []
        assert config.instruments == []
        assert config.macro_indicators == []
        assert config.macro_countries == []

    @pytest.mark.skip("TODO: Add test for invalid config")
    def test_invalid_config(self) -> None:
        """Test validation of invalid config."""
        with pytest.raises(ValidationError):
            EODHDConfig(
                api_key="test_key",
                base_url="not_a_url",  # Invalid URL format
                bucket_name="test-bucket",
            )


class TestBaseEODHDData:
    """Test suite for BaseEODHDData model."""

    class ConcreteData(BaseEODHDData):
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
        assert path == "eodhd/test-data/2024/01/29.json.gz"


class TestExchangeData:
    """Test suite for ExchangeData model."""

    def test_get_exchanges_list(self) -> None:
        """Test extraction of exchange list."""
        test_data: list[ExchangeRecord] = [
            {"Code": "LSE", "Name": "London Stock Exchange"},
            {"Code": "NYSE", "Name": "New York Stock Exchange"},
        ]
        data = ExchangeData(data=cast(JSONType, test_data), timestamp=datetime.now())

        exchanges = data.get_exchanges_list()
        assert exchanges == ["LSE", "NYSE"]

    def test_get_exchanges_list_invalid_data(self) -> None:
        """Test handling of invalid data format."""
        data = ExchangeData(data={"not": "a list"}, timestamp=datetime.now())

        with pytest.raises(ValueError, match="Data is not a list of dictionaries"):
            data.get_exchanges_list()


class TestExchangeSymbolData:
    """Test suite for ExchangeSymbolData model."""

    def test_get_exchange_symbols_list(self) -> None:
        """Test extraction of exchange symbols list."""
        test_data: list[SymbolRecord] = [
            {"Code": "AAPL", "Name": "Apple Inc"},
            {"Code": "GOOGL", "Name": "Alphabet Inc"},
        ]
        data = ExchangeSymbolData(
            data=cast(JSONType, test_data),
            timestamp=datetime.now(),
            exchange="US",
        )

        symbols = data.get_exchange_symbols_list()
        assert symbols == ["AAPL.US", "GOOGL.US"]

    def test_get_exchange_symbols_list_invalid_data(self) -> None:
        """Test handling of invalid data format."""
        data = ExchangeSymbolData(
            data={"not": "a list"},
            timestamp=datetime.now(),
            exchange="US",
        )

        with pytest.raises(ValueError, match="Data is not a list of dictionaries"):
            data.get_exchange_symbols_list()

    def test_metadata(self) -> None:
        """Test metadata includes exchange."""
        data = ExchangeSymbolData(
            data=[],
            timestamp=datetime.now(),
            exchange="US",
        )

        metadata = data._get_metadata()
        assert metadata["exchange"] == "US"

    def test_storage_path(self) -> None:
        """Test storage path includes exchange."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = ExchangeSymbolData(
            data=[],
            timestamp=timestamp,
            exchange="US",
            data_type="exchange-symbol-list",
        )

        path = data.get_storage_path()
        assert path == "eodhd/exchange-symbol-list/2024/01/29/US.json.gz"


class TestInstrumentData:
    """Test suite for InstrumentData model."""

    def test_metadata(self) -> None:
        """Test metadata includes instrument details."""
        data = InstrumentData(
            data={},
            timestamp=datetime.now(),
            code="AAPL",
            exchange="US",
            data_type="eod",
        )

        metadata = data._get_metadata()
        assert metadata["code"] == "AAPL"
        assert metadata["exchange"] == "US"

    def test_storage_path(self) -> None:
        """Test storage path includes instrument details."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = InstrumentData(
            data={},
            timestamp=timestamp,
            code="AAPL",
            exchange="US",
            data_type="eod",
        )

        path = data.get_storage_path()
        assert path == "eodhd/eod/2024/01/29/US/AAPL.json.gz"


class TestMacroData:
    """Test suite for MacroData model."""

    def test_metadata(self) -> None:
        """Test metadata includes macro details."""
        data = MacroData(
            data={},
            timestamp=datetime.now(),
            iso_code="US",
            indicator="GDP",
        )

        metadata = data._get_metadata()
        assert metadata["iso_code"] == "US"
        assert metadata["indicator"] == "GDP"

    def test_storage_path(self) -> None:
        """Test storage path includes macro details."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = MacroData(
            data={},
            timestamp=timestamp,
            iso_code="US",
            indicator="GDP",
        )

        path = data.get_storage_path()
        assert path == "eodhd/macro-indicators/2024/01/29/US/GDP.json.gz"


class TestEconomicEventData:
    """Test suite for EconomicEventData model."""

    def test_data_type(self) -> None:
        """Test data type is set correctly."""
        data = EconomicEventData(data={}, timestamp=datetime.now())
        assert data.data_type == "economic-events"

    def test_storage_path(self) -> None:
        """Test storage path format."""
        timestamp = datetime(2024, 1, 29, 12, 0, 0)
        data = EconomicEventData(data={}, timestamp=timestamp)

        path = data.get_storage_path()
        assert path == "eodhd/economic-events/2024/01/29.json.gz"
