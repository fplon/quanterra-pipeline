from dataclasses import dataclass
from typing import Optional, cast

import httpx
from httpx import Response
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.common.logging.utils import log_retry_attempt
from src.common.types import JSONType


@dataclass
class EODHDClient:
    """EODHD API client for making HTTP requests."""

    api_key: str
    base_url: str

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        before_sleep=log_retry_attempt,
    )
    async def _make_request(self, endpoint: str, params: dict[str, str] | None = None) -> Response:
        """Make HTTP request to EODHD API and return raw response.

        Retries on network timeouts and HTTP errors with exponential backoff:
        - 3 maximum attempts
        - 4-10 second wait between retries
        - Exponential backoff multiplier of 1
        """
        if params is None:
            params = {}

        params["api_token"] = self.api_key
        params["fmt"] = "json"

        url = f"{self.base_url}{endpoint}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                return response

        except httpx.TimeoutException as e:
            logger.warning(f"Request timeout for {endpoint}: {str(e)}")
            raise

        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise

    async def get_exchanges(self) -> JSONType:
        """Get list of available exchanges."""
        response = await self._make_request("exchanges-list")
        return cast(JSONType, response.json())

    async def get_exchange_symbols(
        self, exchange: str, asset_type: Optional[str] = None, delisted: bool = False
    ) -> JSONType:
        """Get list of symbols for a specific exchange."""
        params = {}
        if asset_type:
            params["type"] = asset_type
        if delisted:
            params["delisted"] = "1"

        response = await self._make_request(f"exchange-symbol-list/{exchange}", params)
        return cast(JSONType, response.json())

    async def get_eod_data(
        self,
        instrument: str,
        exchange: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> JSONType:
        """Get end-of-day data for a specific instrument."""
        params = {}
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date

        response = await self._make_request(f"eod/{instrument}.{exchange}", params)
        return cast(JSONType, response.json())

    async def get_fundamentals(self, instrument: str, exchange: str) -> JSONType:
        """Get fundamental data for a specific instrument."""
        response = await self._make_request(f"fundamentals/{instrument}.{exchange}")
        return cast(JSONType, response.json())

    async def get_dividends(self, instrument: str, exchange: str) -> JSONType:
        """Get dividend data for a specific instrument."""
        response = await self._make_request(f"div/{instrument}.{exchange}")
        return cast(JSONType, response.json())

    async def get_splits(self, instrument: str, exchange: str) -> JSONType:
        """Get split history for a specific instrument."""
        response = await self._make_request(f"splits/{instrument}.{exchange}")
        return cast(JSONType, response.json())

    async def get_bulk_eod(self, exchange: str) -> JSONType:
        """Get bulk end-of-day data for an entire exchange."""
        response = await self._make_request(f"eod-bulk-last-day/{exchange}")
        return cast(JSONType, response.json())

    async def get_economic_events(
        self,
        country: str | None = None,
        comparison: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
    ) -> JSONType:
        """Get economic calendar events."""
        params = {
            "limit": str(limit),
        }
        if country:
            params["country"] = country
        if comparison:
            params["comparison"] = comparison
        if start_date:
            params["from"] = start_date
        if end_date:
            params["to"] = end_date
        response = await self._make_request("economic-events", params)
        return cast(JSONType, response.json())

    async def get_macro_indicator(self, iso_code: str, indicator: str) -> JSONType:
        """Get macro indicator data for a country."""
        params = {"indicator": indicator}
        response = await self._make_request(f"macro-indicator/{iso_code}", params)
        return cast(JSONType, response.json())

    async def get_news(self, instrument: str, exchange: str) -> JSONType:
        """Get news for a specific instrument."""
        params = {"s": f"{instrument}.{exchange}"}
        response = await self._make_request("news", params)
        return cast(JSONType, response.json())
