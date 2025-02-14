from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from tenacity import RetryError

from clients.api.eodhd_client import EODHDClient

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_response() -> MagicMock:
    """Create a mock HTTP response."""
    response = MagicMock()
    response.json.return_value = {"test": "data"}
    return response


@pytest.fixture
def client() -> EODHDClient:
    """Create an EODHD client instance."""
    return EODHDClient(api_key="test_key", base_url="https://test.com/api/v1/")


def test_prepare_request_params(client: EODHDClient) -> None:
    """Test parameter preparation."""
    # Test with no params
    params = client._prepare_request_params()
    assert params == {"api_token": "test_key", "fmt": "json"}

    # Test with existing params
    params = client._prepare_request_params({"existing": "param"})
    assert params == {"existing": "param", "api_token": "test_key", "fmt": "json"}


def test_get_headers(client: EODHDClient) -> None:
    """Test header preparation."""
    headers = client._get_headers()
    assert headers == {}


async def test_make_request_success(client: EODHDClient, mock_response: MagicMock) -> None:
    """Test successful API request."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_response

    with patch("httpx.AsyncClient", return_value=mock_client):
        response = await client._make_request("test-endpoint", {"param": "value"})

    assert response == mock_response
    mock_client.__aenter__.return_value.get.assert_called_once_with(
        "https://test.com/api/v1/test-endpoint",
        params={"param": "value", "api_token": "test_key", "fmt": "json"},
        headers={},
    )


async def test_make_request_timeout(client: EODHDClient) -> None:
    """Test request timeout handling."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.side_effect = httpx.TimeoutException("Timeout")

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(RetryError):
            await client._make_request("test-endpoint")


async def test_make_request_http_error(client: EODHDClient) -> None:
    """Test HTTP error handling."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.side_effect = httpx.HTTPError("HTTP Error")

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(RetryError):
            await client._make_request("test-endpoint")


async def test_get_exchanges(client: EODHDClient) -> None:
    """Test getting exchanges list."""
    mock_response = {"exchanges": ["NYSE", "LSE"]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_exchanges()
        assert result == mock_response
        mock_request.assert_called_once_with("exchanges-list")


async def test_get_exchange_symbols(client: EODHDClient) -> None:
    """Test getting exchange symbols."""
    mock_response = {"symbols": ["AAPL", "MSFT"]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        # Test with minimal parameters
        result = await client.get_exchange_symbols("NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("exchange-symbol-list/NYSE", {})

        # Test with all parameters
        mock_request.reset_mock()
        result = await client.get_exchange_symbols("NYSE", "stock", True)
        assert result == mock_response
        mock_request.assert_called_once_with(
            "exchange-symbol-list/NYSE", {"type": "stock", "delisted": "1"}
        )


async def test_get_eod_data(client: EODHDClient) -> None:
    """Test getting EOD data."""
    mock_response = {"eod": [{"date": "2024-01-26", "close": 100.0}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        # Test with minimal parameters
        result = await client.get_eod_data("AAPL", "NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("eod/AAPL.NYSE", {})

        # Test with all parameters
        mock_request.reset_mock()
        result = await client.get_eod_data("AAPL", "NYSE", "2024-01-01", "2024-01-31")
        assert result == mock_response
        mock_request.assert_called_once_with(
            "eod/AAPL.NYSE", {"from": "2024-01-01", "to": "2024-01-31"}
        )


async def test_get_fundamentals(client: EODHDClient) -> None:
    """Test getting fundamentals data."""
    mock_response = {"fundamentals": {"market_cap": 1000000}}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_fundamentals("AAPL", "NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("fundamentals/AAPL.NYSE")


async def test_get_dividends(client: EODHDClient) -> None:
    """Test getting dividends data."""
    mock_response = {"dividends": [{"date": "2024-01-26", "value": 0.5}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_dividends("AAPL", "NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("div/AAPL.NYSE")


async def test_get_splits(client: EODHDClient) -> None:
    """Test getting splits data."""
    mock_response = {"splits": [{"date": "2024-01-26", "ratio": "4:1"}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_splits("AAPL", "NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("splits/AAPL.NYSE")


async def test_get_bulk_eod(client: EODHDClient) -> None:
    """Test getting bulk EOD data."""
    mock_response = {"bulk_data": [{"symbol": "AAPL", "close": 100.0}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_bulk_eod("NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("eod-bulk-last-day/NYSE", {})


async def test_get_economic_events(client: EODHDClient) -> None:
    """Test getting economic events."""
    mock_response = {"events": [{"date": "2024-01-26", "event": "GDP Release"}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        # Test with minimal parameters
        result = await client.get_economic_events()
        assert result == mock_response
        mock_request.assert_called_once_with("economic-events", {"limit": "1000"})

        # Test with all parameters
        mock_request.reset_mock()
        result = await client.get_economic_events(
            country="US",
            comparison="actual",
            start_date="2024-01-01",
            end_date="2024-01-31",
            limit=500,
        )
        assert result == mock_response
        mock_request.assert_called_once_with(
            "economic-events",
            {
                "limit": "500",
                "country": "US",
                "comparison": "actual",
                "from": "2024-01-01",
                "to": "2024-01-31",
            },
        )


async def test_get_macro_indicator(client: EODHDClient) -> None:
    """Test getting macro indicator data."""
    mock_response = {"indicator": {"value": 100.0}}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_macro_indicator("US", "GDP")
        assert result == mock_response
        mock_request.assert_called_once_with("macro-indicator/US", {"indicator": "GDP"})


async def test_get_news(client: EODHDClient) -> None:
    """Test getting news data."""
    mock_response = {"news": [{"date": "2024-01-26", "title": "Test News"}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_news("AAPL", "NYSE")
        assert result == mock_response
        mock_request.assert_called_once_with("news", {"s": "AAPL.NYSE"})
