from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType
from src.ingest.data_sources.oanda.models import (
    InstrumentsData,
    OANDAConfig,
    StorageLocation,
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
        instruments=["EUR_USD", "GBP_USD"],
        granularity="H1",
        count=1000,
    )


@pytest.fixture
def mock_storage_client() -> MagicMock:
    """Create a mock storage client for testing."""
    return MagicMock(spec=GCPStorageClient)


@pytest.fixture
def mock_oanda_client() -> AsyncMock:
    """Create a mock OANDA client for testing."""
    return AsyncMock()


class TestInstrumentsProcessor:
    """Test suite for InstrumentsProcessor."""

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock, mock_oanda_client: AsyncMock
    ) -> InstrumentsProcessor:
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
    ) -> None:
        """Test successful processing of instruments data."""
        mock_data: JSONType = {
            "instruments": [
                {"name": "EUR_USD", "type": "CURRENCY"},
                {"name": "GBP_USD", "type": "CURRENCY"},
            ]
        }
        mock_oanda_client.get_instruments.return_value = mock_data

        result = await processor.process()

        assert len(result) == 1
        assert isinstance(result[0], StorageLocation)
        mock_oanda_client.get_instruments.assert_called_once()
        mock_storage_client.store_json_data.assert_called_once()
        stored_data = mock_storage_client.store_json_data.call_args[1]["data"]
        assert stored_data["data"] == mock_data
        assert "metadata" in stored_data

    @pytest.mark.asyncio
    async def test_process_failure(
        self, processor: InstrumentsProcessor, mock_oanda_client: AsyncMock
    ) -> None:
        """Test handling of API failure when processing instruments."""
        mock_oanda_client.get_instruments.side_effect = Exception("API Error")

        with pytest.raises(Exception):
            await processor.process()


class TestCandlesProcessor:
    """Test suite for CandlesProcessor."""

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock, mock_oanda_client: AsyncMock
    ) -> CandlesProcessor:
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
    ) -> None:
        """Test successful processing of candles data."""
        mock_data: JSONType = {
            "candles": [
                {
                    "time": "2024-01-01T00:00:00Z",
                    "bid": {"o": "1.1000", "h": "1.1100", "l": "1.0900", "c": "1.1050"},
                }
            ]
        }
        mock_oanda_client.get_candles.return_value = mock_data

        result = await processor.process()

        assert len(result) == len(mock_config.instruments)
        assert all(isinstance(loc, StorageLocation) for loc in result)
        assert mock_oanda_client.get_candles.call_count == len(mock_config.instruments)
        assert mock_storage_client.store_json_data.call_count == len(mock_config.instruments)

        # Verify the candles API was called with correct parameters
        mock_oanda_client.get_candles.assert_called_with(
            instrument=mock_config.instruments[-1],
            granularity=mock_config.granularity,
            count=mock_config.count,
            price=mock_config.price,
        )

    @pytest.mark.asyncio
    async def test_process_partial_failure(
        self,
        processor: CandlesProcessor,
        mock_oanda_client: AsyncMock,
        mock_config: OANDAConfig,
    ) -> None:
        """Test handling of partial failures when processing candles."""
        mock_data: JSONType = {"candles": []}
        mock_oanda_client.get_candles.side_effect = [
            mock_data,  # Success for first instrument
            Exception("API Error"),  # Failure for second instrument
        ]

        result = await processor.process()

        assert len(result) == 1  # Only one successful result
        assert isinstance(result[0], StorageLocation)
        assert mock_oanda_client.get_candles.call_count == len(mock_config.instruments)


class TestOANDAIngestionProcessor:
    """Test suite for base OANDAIngestionProcessor."""

    class ConcreteProcessor(OANDAIngestionProcessor):
        """Concrete implementation for testing abstract base class."""

        async def process(self) -> list[StorageLocation]:
            return []

    @pytest.fixture
    def processor(
        self, mock_config: OANDAConfig, mock_storage_client: MagicMock
    ) -> "TestOANDAIngestionProcessor.ConcreteProcessor":
        with patch("src.ingest.data_sources.oanda.processors.GCPStorageClient") as mock_gcp:
            mock_gcp.return_value = mock_storage_client
            return TestOANDAIngestionProcessor.ConcreteProcessor(mock_config)

    @pytest.mark.asyncio
    async def test_store_data_no_client(
        self, processor: "TestOANDAIngestionProcessor.ConcreteProcessor"
    ) -> None:
        """Test handling of missing storage client."""
        processor.storage_client = None  # type: ignore
        data = InstrumentsData(data={"test": "data"}, timestamp=datetime.now())
        location = StorageLocation(bucket="test-bucket", path="test/path")

        with pytest.raises(RuntimeError, match="Storage client not initialised"):
            await processor._store_data(data, location)

    @pytest.mark.asyncio
    async def test_store_data_success(
        self,
        processor: "TestOANDAIngestionProcessor.ConcreteProcessor",
        mock_storage_client: MagicMock,
    ) -> None:
        """Test successful data storage."""
        data = InstrumentsData(data={"test": "data"}, timestamp=datetime.now())
        location = StorageLocation(bucket="test-bucket", path="test/path")

        await processor._store_data(data, location)

        mock_storage_client.store_json_data.assert_called_once_with(
            data=data.to_json(),
            bucket_name=location.bucket,
            blob_path=location.path,
            compress=True,
        )
