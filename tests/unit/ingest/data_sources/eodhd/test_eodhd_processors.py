from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType
from src.ingest.data_sources.eodhd.models import (
    EODHDConfig,
    ExchangeData,
    StorageLocation,
)
from src.ingest.data_sources.eodhd.processors import (
    EconomicEventDataProcessor,
    EODIngestionProcessor,
    ExchangeDataProcessor,
    ExchangeSymbolDataProcessor,
    InstrumentDataProcessor,
    MacroDataProcessor,
)


@pytest.fixture
def mock_config() -> EODHDConfig:
    """Create a mock config for testing."""
    return EODHDConfig(
        api_key="test_key",
        base_url="http://test.com",
        bucket_name="test-bucket",
        exchanges=["LSE", "NYSE"],
        instruments=["AAPL.US", "TSLA.US"],
        macro_indicators=["GDP", "CPI"],
        macro_countries=["USA", "GBR"],
    )


@pytest.fixture
def mock_storage_client() -> MagicMock:
    """Create a mock storage client for testing."""
    return MagicMock(spec=GCPStorageClient)


@pytest.fixture
def mock_eodhd_client() -> AsyncMock:
    """Create a mock EODHD client for testing."""
    return AsyncMock()


class TestExchangeDataProcessor:
    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock, mock_eodhd_client: AsyncMock
    ) -> ExchangeDataProcessor:
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.eodhd.processors.EODHDClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_eodhd_client
                return ExchangeDataProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: ExchangeDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
    ) -> None:
        mock_data: JSONType = {"exchanges": [{"id": "LSE", "name": "London Stock Exchange"}]}
        mock_eodhd_client.get_exchanges.return_value = mock_data

        result = await processor.process()

        assert len(result) == 1
        assert isinstance(result[0], StorageLocation)
        mock_eodhd_client.get_exchanges.assert_called_once()
        mock_storage_client.store_json_data.assert_called_once()
        stored_data = mock_storage_client.store_json_data.call_args[1]["data"]
        assert stored_data["data"] == mock_data
        assert "metadata" in stored_data

    @pytest.mark.asyncio
    async def test_process_failure(
        self, processor: ExchangeDataProcessor, mock_eodhd_client: AsyncMock
    ) -> None:
        mock_eodhd_client.get_exchanges.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process()


class TestExchangeSymbolProcessor:
    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock, mock_eodhd_client: AsyncMock
    ) -> ExchangeSymbolDataProcessor:
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.eodhd.processors.EODHDClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_eodhd_client
                return ExchangeSymbolDataProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: ExchangeSymbolDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
        mock_config: EODHDConfig,
    ) -> None:
        mock_data: JSONType = [{"code": "AAPL", "name": "Apple Inc"}]
        mock_eodhd_client.get_exchange_symbols.return_value = mock_data

        result = await processor.process()

        assert len(result) == len(mock_config.exchanges)  # One for each exchange in config
        assert all(isinstance(loc, StorageLocation) for loc in result)
        assert mock_eodhd_client.get_exchange_symbols.call_count == len(mock_config.exchanges)
        assert mock_storage_client.store_json_data.call_count == len(mock_config.exchanges)

    @pytest.mark.asyncio
    async def test_process_partial_failure(
        self,
        processor: ExchangeSymbolDataProcessor,
        mock_eodhd_client: AsyncMock,
    ) -> None:
        mock_eodhd_client.get_exchange_symbols.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process()


class TestInstrumentDataProcessor:
    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock, mock_eodhd_client: AsyncMock
    ) -> InstrumentDataProcessor:
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.eodhd.processors.EODHDClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_eodhd_client
                return InstrumentDataProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: InstrumentDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
        mock_config: EODHDConfig,
    ) -> None:
        mock_data: JSONType = {"price": 150.0, "volume": 1000000}
        mock_eodhd_client.get_eod_data.return_value = mock_data
        mock_eodhd_client.get_dividends.return_value = mock_data
        mock_eodhd_client.get_splits.return_value = mock_data
        mock_eodhd_client.get_fundamentals.return_value = mock_data
        mock_eodhd_client.get_news.return_value = mock_data

        result = await processor.process()

        expected_calls = len(processor._data_type_handlers) * len(mock_config.instruments)
        assert len(result) == expected_calls
        assert all(isinstance(loc, StorageLocation) for loc in result)
        assert mock_storage_client.store_json_data.call_count == expected_calls

    @pytest.mark.asyncio
    async def test_fetch_data_by_type_invalid_type(
        self, processor: InstrumentDataProcessor
    ) -> None:
        with pytest.raises(ValueError):
            await processor._fetch_data_by_type("AAPL", "US", "invalid_type")

    @pytest.mark.asyncio
    async def test_process_continues_on_error(
        self,
        processor: InstrumentDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
        mock_config: EODHDConfig,
    ) -> None:
        mock_data: JSONType = {"price": 150.0}
        mock_eodhd_client.get_eod_data.return_value = mock_data
        mock_eodhd_client.get_dividends.side_effect = Exception("API Error")
        mock_eodhd_client.get_splits.return_value = mock_data

        result = await processor.process()

        assert len(result) > 0  # Some successful calls should still produce results
        assert len(result) < len(processor._data_type_handlers) * len(mock_config.instruments)


class TestMacroDataProcessor:
    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock, mock_eodhd_client: AsyncMock
    ) -> MacroDataProcessor:
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.eodhd.processors.EODHDClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_eodhd_client
                return MacroDataProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: MacroDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
        mock_config: EODHDConfig,
    ) -> None:
        mock_data: JSONType = {"value": 2.5, "date": "2024-01-01"}
        mock_eodhd_client.get_macro_indicator.return_value = mock_data

        result = await processor.process()

        expected_calls = len(mock_config.macro_indicators) * len(mock_config.macro_countries)
        assert len(result) == expected_calls
        assert all(isinstance(loc, StorageLocation) for loc in result)
        assert mock_storage_client.store_json_data.call_count == expected_calls

    @pytest.mark.asyncio
    async def test_process_continues_on_error(
        self,
        processor: MacroDataProcessor,
        mock_eodhd_client: AsyncMock,
    ) -> None:
        mock_eodhd_client.get_macro_indicator.side_effect = Exception("API Error")

        result = await processor.process()

        assert len(result) == 0


class TestEconomicEventDataProcessor:
    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock, mock_eodhd_client: AsyncMock
    ) -> EconomicEventDataProcessor:
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            with patch("src.ingest.data_sources.eodhd.processors.EODHDClient") as mock_client:
                mock_gcp.return_value = mock_storage_client
                mock_client.return_value = mock_eodhd_client
                return EconomicEventDataProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_process_success(
        self,
        processor: EconomicEventDataProcessor,
        mock_eodhd_client: AsyncMock,
        mock_storage_client: MagicMock,
    ) -> None:
        mock_data: JSONType = [{"event": "GDP Release", "date": "2024-01-01"}]
        mock_eodhd_client.get_economic_events.return_value = mock_data

        result = await processor.process()

        assert len(result) == 1
        assert isinstance(result[0], StorageLocation)
        mock_eodhd_client.get_economic_events.assert_called_once()
        mock_storage_client.store_json_data.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_failure(
        self, processor: EconomicEventDataProcessor, mock_eodhd_client: AsyncMock
    ) -> None:
        mock_eodhd_client.get_economic_events.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process()


class TestEODIngestionProcessor:
    class ConcreteProcessor(EODIngestionProcessor):
        async def process(self) -> list[StorageLocation]:
            return []

    @pytest.fixture
    def processor(
        self, mock_config: EODHDConfig, mock_storage_client: MagicMock
    ) -> "TestEODIngestionProcessor.ConcreteProcessor":
        with patch("src.ingest.data_sources.eodhd.processors.GCPStorageClient") as mock_gcp:
            mock_gcp.return_value = mock_storage_client
            return self.ConcreteProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_store_data_no_client(
        self, processor: "TestEODIngestionProcessor.ConcreteProcessor"
    ) -> None:
        processor.storage_client = None  # type: ignore
        data = ExchangeData(data={}, timestamp=datetime.now())
        location = StorageLocation(bucket="test", path="test")

        with pytest.raises(RuntimeError):
            await processor._store_data(data, location)

    @pytest.mark.asyncio
    async def test_store_data_success(
        self,
        processor: "TestEODIngestionProcessor.ConcreteProcessor",
        mock_storage_client: MagicMock,
    ) -> None:
        data = ExchangeData(data={}, timestamp=datetime.now())
        location = StorageLocation(bucket="test", path="test")

        await processor._store_data(data, location)

        mock_storage_client.store_json_data.assert_called_once()
        call_kwargs = mock_storage_client.store_json_data.call_args[1]
        assert call_kwargs["bucket_name"] == location.bucket
        assert call_kwargs["blob_path"] == location.path
        assert call_kwargs["compress"] is True
