from dataclasses import dataclass
from typing import cast

import httpx
from httpx import Response
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.common.logging.utils import log_retry_attempt
from src.common.types import JSONType


@dataclass
class OANDAClient:
    """OANDA API client for making HTTP requests."""

    api_key: str
    account_id: str
    base_url: str

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        before_sleep=log_retry_attempt,
    )
    async def _make_request(
        self, endpoint: str, params: dict[str, str | int] | None = None
    ) -> Response:
        """Make HTTP request to OANDA API and return raw response."""
        if params is None:
            params = {}

        url = f"{self.base_url}/{endpoint}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params, headers=self._get_headers())
                response.raise_for_status()
                return response

        except httpx.TimeoutException as e:
            logger.warning(f"Request timeout for {endpoint}: {str(e)}")
            raise

        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {endpoint}: {str(e)}")
            raise

    def _get_headers(self) -> dict[str, str]:
        """Get headers for OANDA API requests."""
        return {"Authorization": f"Bearer {self.api_key}"}

    async def get_instruments(self) -> JSONType:
        """Get list of available instruments."""
        response = await self._make_request(f"accounts/{self.account_id}/instruments")
        return cast(JSONType, response.json())

    async def get_candles(
        self, instrument: str, granularity: str, count: int, price: str = "MBA"
    ) -> JSONType:
        """Get candles for a specific instrument and time period."""
        params: dict[str, str | int] = {"granularity": granularity, "count": count, "price": price}
        response = await self._make_request(f"instruments/{instrument}/candles", params)
        return cast(JSONType, response.json())
