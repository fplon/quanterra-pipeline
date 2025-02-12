from types import TracebackType
from typing import Generator

import yfinance as yf
from pandas import DataFrame
from requests import Session

from src.common.types import JSONType


class YahooFinanceClient:
    """Yahoo Finance client using yfinance package."""

    def __init__(self) -> None:
        """Initialise client."""
        self._session = Session()
        self._session.headers["User-agent"] = "quanterra/1.0"

    def __enter__(self) -> "YahooFinanceClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session.close()

    def get_tickers_data(self, yf_tickers: list[str]) -> JSONType:
        """Get data for a list of tickers."""
        tickers = yf.Tickers(" ".join(yf_tickers), session=self._session)
        return {
            "info": {symbol: ticker.info for symbol, ticker in tickers.tickers.items()},
            "financials": {
                symbol: {
                    "balance_sheet": ticker.balance_sheet.to_dict(),
                    "cash_flow": ticker.cashflow.to_dict(),
                    "income_stmt": ticker.income_stmt.to_dict(),
                }
                for symbol, ticker in tickers.tickers.items()
            },
            "dividends": {
                symbol: ticker.dividends.to_dict() for symbol, ticker in tickers.tickers.items()
            },
            "actions": {
                symbol: ticker.actions.to_dict() for symbol, ticker in tickers.tickers.items()
            },
        }

    @staticmethod
    def chunk_list(lst: list[str], chunk_size: int) -> Generator[list[str], None, None]:
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def get_market_data(
        self, yf_tickers: list[str], period: str = "max", interval: str = "1d"
    ) -> JSONType:
        """Get historical market data for multiple tickers in chunks."""
        all_data: dict[str, dict[str, dict[str, float]]] = {}

        for chunk in self.chunk_list(yf_tickers, 5):  # Process 5 tickers at a time
            df: DataFrame = yf.download(
                chunk,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                prepost=True,
                threads=False,
                proxy=None,
                session=self._session,
                progress=False,
            )
            chunk_data = self._market_data_to_dict(df, chunk)
            if isinstance(chunk_data, dict):
                all_data.update(chunk_data)

        return all_data

    def _market_data_to_dict(self, df: DataFrame, yf_tickers: list[str]) -> JSONType:
        """Convert market data dataframe to dictionary."""
        result: dict[str, dict[str, dict[str, float]]] = {}  # FIXME timestamp keys

        for yf_ticker in yf_tickers:
            result[yf_ticker] = {
                field: df[yf_ticker][field].to_dict()
                for field in ["Open", "High", "Low", "Close", "Volume"]
            }

        return result
