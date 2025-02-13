from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from tenacity import RetryError

from src.clients.oanda_client import OANDAClient

pytestmark = pytest.mark.asyncio


@pytest.fixture
def mock_response() -> MagicMock:
    """Create a mock HTTP response."""
    response = MagicMock()
    response.json.return_value = {"test": "data"}
    return response


@pytest.fixture
def client() -> OANDAClient:
    """Create an OANDA client instance."""
    return OANDAClient(
        api_key="test_key", base_url="https://test.com/api/v1/", account_id="test_account"
    )


def test_get_headers(client: OANDAClient) -> None:
    """Test header preparation."""
    headers = client._get_headers()
    assert headers == {"Authorization": "Bearer test_key"}


async def test_make_request_success(client: OANDAClient, mock_response: MagicMock) -> None:
    """Test successful API request."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.return_value = mock_response

    with patch("httpx.AsyncClient", return_value=mock_client):
        response = await client._make_request("test-endpoint", {"param": "value"})

    assert response == mock_response
    mock_client.__aenter__.return_value.get.assert_called_once_with(
        "https://test.com/api/v1/test-endpoint",
        params={"param": "value"},
        headers={"Authorization": "Bearer test_key"},
    )


async def test_make_request_timeout(client: OANDAClient) -> None:
    """Test request timeout handling."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.side_effect = httpx.TimeoutException("Timeout")

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(RetryError):
            await client._make_request("test-endpoint")


async def test_make_request_http_error(client: OANDAClient) -> None:
    """Test HTTP error handling."""
    mock_client = AsyncMock()
    mock_client.__aenter__.return_value.get.side_effect = httpx.HTTPError("HTTP Error")

    with patch("httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(RetryError):
            await client._make_request("test-endpoint")


async def test_get_instruments(client: OANDAClient) -> None:
    """Test getting instruments list."""
    mock_response = {"instruments": [{"name": "EUR_USD", "type": "CURRENCY"}]}
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        result = await client.get_instruments()
        assert result == mock_response
        mock_request.assert_called_once_with(f"accounts/{client.account_id}/instruments")


async def test_get_candles(client: OANDAClient) -> None:
    """Test getting candles data."""
    mock_response = {
        "candles": [
            {
                "time": "2024-01-26T00:00:00Z",
                "mid": {"o": "1.08", "h": "1.09", "l": "1.07", "c": "1.08"},
            }
        ]
    }
    mock_request = AsyncMock()
    mock_request.return_value = MagicMock()
    mock_request.return_value.json.return_value = mock_response

    with patch.object(client, "_make_request", mock_request):
        # Test with default price
        result = await client.get_candles("EUR_USD", "H1", 100)
        assert result == mock_response
        mock_request.assert_called_once_with(
            "instruments/EUR_USD/candles", {"granularity": "H1", "count": 100, "price": "MBA"}
        )

        # Test with custom price
        mock_request.reset_mock()
        result = await client.get_candles("EUR_USD", "H1", 100, "B")
        assert result == mock_response
        mock_request.assert_called_once_with(
            "instruments/EUR_USD/candles", {"granularity": "H1", "count": 100, "price": "B"}
        )
