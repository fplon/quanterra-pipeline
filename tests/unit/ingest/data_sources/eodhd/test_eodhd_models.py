from datetime import datetime
from typing import Any, cast

import pytest

from src.common.types import JSONType
from src.ingest.data_sources.eodhd.models import (
    BaseEODHDData,
    EconomicEventData,
    EODHDConfig,
    ExchangeData,
    ExchangeSymbolData,
    InstrumentData,
    MacroData,
    StorageLocation,
)


@pytest.fixture
def sample_json_data() -> JSONType:
    return {"key": "value", "nested": {"data": 123}}


@pytest.fixture
def sample_timestamp() -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0)


class TestEODHDConfig:
    def test_config_initialisation(self) -> None:
        config = EODHDConfig(
            api_key="test_key",
            base_url="http://test.com",
            bucket_name="test-bucket",
            exchanges=["NYSE", "LSE"],
            instruments=["AAPL.US", "GOOGL.US"],
            macro_indicators=["GDP", "CPI"],
            macro_countries=["USA", "GBR"],
        )

        assert config.api_key == "test_key"
        assert config.base_url == "http://test.com"
        assert config.bucket_name == "test-bucket"
        assert config.exchanges == ["NYSE", "LSE"]
        assert config.instruments == ["AAPL.US", "GOOGL.US"]
        assert config.macro_indicators == ["GDP", "CPI"]
        assert config.macro_countries == ["USA", "GBR"]


class TestBaseEODHDData:
    def test_to_json(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        base_data = BaseEODHDData(
            data=sample_json_data, timestamp=sample_timestamp, data_type="test-type"
        )

        json_output = cast(dict[str, Any], base_data.to_json())
        assert json_output["data"] == sample_json_data
        assert json_output["metadata"]["data_type"] == "test-type"
        assert json_output["metadata"]["timestamp"] == "2024-01-01T12:00:00"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        base_data = BaseEODHDData(
            data=sample_json_data, timestamp=sample_timestamp, data_type="test-type"
        )

        expected_path = "eodhd/test-type/2024/01/01.json.gz"
        assert base_data.get_storage_path() == expected_path


class TestExchangeData:
    def test_exchange_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        exchange_data = ExchangeData(data=sample_json_data, timestamp=sample_timestamp)

        assert exchange_data.data == sample_json_data
        assert exchange_data.timestamp == sample_timestamp
        assert exchange_data.data_type == "exchanges-list"


class TestExchangeSymbolData:
    def test_exchange_symbol_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        exchange_symbol_data = ExchangeSymbolData(
            data=sample_json_data, exchange="NYSE", timestamp=sample_timestamp
        )

        assert exchange_symbol_data.data == sample_json_data
        assert exchange_symbol_data.exchange == "NYSE"
        assert exchange_symbol_data.timestamp == sample_timestamp
        assert exchange_symbol_data.data_type == "exchange-symbol-list"

    def test_get_metadata(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        exchange_symbol_data = ExchangeSymbolData(
            data=sample_json_data, exchange="NYSE", timestamp=sample_timestamp
        )

        metadata = exchange_symbol_data._get_metadata()
        assert metadata["data_type"] == "exchange-symbol-list"
        assert metadata["timestamp"] == "2024-01-01T12:00:00"
        assert metadata["exchange"] == "NYSE"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        exchange_symbol_data = ExchangeSymbolData(
            data=sample_json_data, exchange="NYSE", timestamp=sample_timestamp
        )

        expected_path = "eodhd/exchange-symbol-list/2024/01/01/NYSE.json.gz"
        assert exchange_symbol_data.get_storage_path() == expected_path


class TestInstrumentData:
    def test_instrument_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        instrument_data = InstrumentData(
            data=sample_json_data,
            code="AAPL",
            exchange="NYSE",
            data_type="eod",
            timestamp=sample_timestamp,
        )

        assert instrument_data.data == sample_json_data
        assert instrument_data.code == "AAPL"
        assert instrument_data.exchange == "NYSE"
        assert instrument_data.data_type == "eod"
        assert instrument_data.timestamp == sample_timestamp

    def test_get_metadata(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        instrument_data = InstrumentData(
            data=sample_json_data,
            code="AAPL",
            exchange="NYSE",
            data_type="eod",
            timestamp=sample_timestamp,
        )

        metadata = instrument_data._get_metadata()
        assert metadata["data_type"] == "eod"
        assert metadata["timestamp"] == "2024-01-01T12:00:00"
        assert metadata["code"] == "AAPL"
        assert metadata["exchange"] == "NYSE"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        instrument_data = InstrumentData(
            data=sample_json_data,
            code="AAPL",
            exchange="NYSE",
            data_type="eod",
            timestamp=sample_timestamp,
        )

        expected_path = "eodhd/eod/2024/01/01/NYSE/AAPL.json.gz"
        assert instrument_data.get_storage_path() == expected_path


class TestMacroData:
    def test_macro_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        macro_data = MacroData(
            data=sample_json_data, iso_code="USA", indicator="GDP", timestamp=sample_timestamp
        )

        assert macro_data.data == sample_json_data
        assert macro_data.iso_code == "USA"
        assert macro_data.indicator == "GDP"
        assert macro_data.timestamp == sample_timestamp
        assert macro_data.data_type == "macro-indicators"

    def test_get_metadata(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        macro_data = MacroData(
            data=sample_json_data, iso_code="USA", indicator="GDP", timestamp=sample_timestamp
        )

        metadata = macro_data._get_metadata()
        assert metadata["data_type"] == "macro-indicators"
        assert metadata["timestamp"] == "2024-01-01T12:00:00"
        assert metadata["iso_code"] == "USA"
        assert metadata["indicator"] == "GDP"

    def test_get_storage_path(self, sample_json_data: JSONType, sample_timestamp: datetime) -> None:
        macro_data = MacroData(
            data=sample_json_data, iso_code="USA", indicator="GDP", timestamp=sample_timestamp
        )

        expected_path = "eodhd/macro-indicators/2024/01/01/USA/GDP.json.gz"
        assert macro_data.get_storage_path() == expected_path


class TestEconomicEventData:
    def test_economic_event_data_initialisation(
        self, sample_json_data: JSONType, sample_timestamp: datetime
    ) -> None:
        economic_event_data = EconomicEventData(data=sample_json_data, timestamp=sample_timestamp)

        assert economic_event_data.data == sample_json_data
        assert economic_event_data.timestamp == sample_timestamp
        assert economic_event_data.data_type == "economic-events"


class TestStorageLocation:
    def test_storage_location_string_representation(self) -> None:
        location = StorageLocation(bucket="test-bucket", path="test/path.json")
        assert str(location) == "test-bucket/test/path.json"
