"""Unit tests for Yahoo Finance client."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from requests import Session

from src.clients.yahoo_finance_client import YahooFinanceClient


@pytest.fixture
def mock_session() -> MagicMock:
    """Create a mock session."""
    mock = MagicMock(spec=Session)
    mock.headers = {}  # Add headers dictionary to mock
    return mock


@pytest.fixture
def client(mock_session: MagicMock) -> YahooFinanceClient:
    """Create a YahooFinanceClient instance with mocked session."""
    with patch("src.clients.yahoo_finance_client.Session", return_value=mock_session):
        return YahooFinanceClient()


class TestYahooFinanceClient:
    """Test suite for YahooFinanceClient."""

    def test_context_manager(self, client: YahooFinanceClient, mock_session: MagicMock) -> None:
        """Test context manager functionality."""
        with client as c:
            assert isinstance(c, YahooFinanceClient)
        mock_session.close.assert_called_once()

    def test_context_manager_with_exception(
        self, client: YahooFinanceClient, mock_session: MagicMock
    ) -> None:
        """Test context manager handles exceptions correctly."""
        with pytest.raises(ValueError):
            with client:
                raise ValueError("Test exception")
        mock_session.close.assert_called_once()

    @patch("src.clients.yahoo_finance_client.yf.Tickers")
    def test_get_tickers_data(
        self, mock_tickers: MagicMock, client: YahooFinanceClient, mock_session: MagicMock
    ) -> None:
        """Test getting ticker data for multiple symbols."""
        # Setup mock data
        mock_ticker1 = MagicMock()
        mock_ticker1.info = {"symbol": "AAPL", "name": "Apple Inc"}
        mock_ticker1.balance_sheet = pd.DataFrame({"2023": {"assets": 1000}})
        mock_ticker1.cashflow = pd.DataFrame({"2023": {"operating_cash": 500}})
        mock_ticker1.income_stmt = pd.DataFrame({"2023": {"revenue": 2000}})
        mock_ticker1.dividends = pd.Series({"2023-01-01": 0.5})
        mock_ticker1.actions = pd.Series({"2023-01-01": 0.5})

        mock_ticker2 = MagicMock()
        mock_ticker2.info = {"symbol": "MSFT", "name": "Microsoft Corp"}
        mock_ticker2.balance_sheet = pd.DataFrame({"2023": {"assets": 2000}})
        mock_ticker2.cashflow = pd.DataFrame({"2023": {"operating_cash": 1000}})
        mock_ticker2.income_stmt = pd.DataFrame({"2023": {"revenue": 3000}})
        mock_ticker2.dividends = pd.Series({"2023-01-01": 0.6})
        mock_ticker2.actions = pd.Series({"2023-01-01": 0.6})

        mock_tickers.return_value.tickers = {"AAPL": mock_ticker1, "MSFT": mock_ticker2}

        # Execute
        result = client.get_tickers_data(["AAPL", "MSFT"])

        # Verify
        mock_tickers.assert_called_once_with("AAPL MSFT", session=mock_session)
        assert isinstance(result, dict)
        assert set(result.keys()) == {"info", "financials", "dividends", "actions"}
        assert set(result["info"].keys()) == {"AAPL", "MSFT"}
        assert result["info"]["AAPL"] == {"symbol": "AAPL", "name": "Apple Inc"}
        assert result["financials"]["AAPL"]["balance_sheet"] == {"2023": {"assets": 1000}}

    @patch("src.clients.yahoo_finance_client.yf.download")
    def test_get_market_data(
        self, mock_download: MagicMock, client: YahooFinanceClient, mock_session: MagicMock
    ) -> None:
        """Test getting market data for multiple symbols."""
        # Setup mock data
        mock_data = pd.DataFrame(
            {
                ("AAPL", "Open"): {pd.Timestamp("2023-01-01"): 150.0},
                ("AAPL", "High"): {pd.Timestamp("2023-01-01"): 155.0},
                ("AAPL", "Low"): {pd.Timestamp("2023-01-01"): 148.0},
                ("AAPL", "Close"): {pd.Timestamp("2023-01-01"): 152.0},
                ("AAPL", "Volume"): {pd.Timestamp("2023-01-01"): 1000000.0},
            }
        )
        mock_download.return_value = mock_data

        # Execute
        result = client.get_market_data(["AAPL"], period="1mo", interval="1d")

        # Verify
        mock_download.assert_called_once_with(
            ["AAPL"],
            period="1mo",
            interval="1d",
            group_by="ticker",
            auto_adjust=True,
            prepost=True,
            threads=False,
            proxy=None,
            session=mock_session,
            progress=False,
        )
        assert isinstance(result, dict)
        assert "AAPL" in result
        assert set(result["AAPL"].keys()) == {"Open", "High", "Low", "Close", "Volume"}

    def test_market_data_to_dict(self, client: YahooFinanceClient) -> None:
        """Test converting market data DataFrame to dictionary."""
        # Setup test data
        test_data = pd.DataFrame(
            {
                ("AAPL", "Open"): {pd.Timestamp("2023-01-01"): 150.0},
                ("AAPL", "High"): {pd.Timestamp("2023-01-01"): 155.0},
                ("AAPL", "Low"): {pd.Timestamp("2023-01-01"): 148.0},
                ("AAPL", "Close"): {pd.Timestamp("2023-01-01"): 152.0},
                ("AAPL", "Volume"): {pd.Timestamp("2023-01-01"): 1000000.0},
            }
        )

        # Execute
        result = client._market_data_to_dict(test_data, ["AAPL"])

        # Verify
        assert isinstance(result, dict)
        assert "AAPL" in result
        assert set(result["AAPL"].keys()) == {"Open", "High", "Low", "Close", "Volume"}
        assert isinstance(result["AAPL"]["Open"], dict)
        assert pd.Timestamp("2023-01-01") in result["AAPL"]["Open"]
        assert result["AAPL"]["Open"][pd.Timestamp("2023-01-01")] == 150.0
