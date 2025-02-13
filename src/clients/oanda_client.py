from dataclasses import dataclass
from typing import cast

from src.clients.abstract.base_api_client import BaseAPIClient
from src.models.data.json_objects import JSONType  # TODO better implementation


@dataclass
class OANDAClient(BaseAPIClient):
    """OANDA API client for making HTTP requests."""

    account_id: str  # Additional field specific to OANDA

    async def get_instruments(self) -> JSONType:
        """Get list of available instruments."""
        response = await self._make_request(f"accounts/{self.account_id}/instruments")
        return cast(JSONType, response.json())

    async def get_candles(
        self, instrument: str, granularity: str, count: int, price: str = "MBA"
    ) -> JSONType:
        """Get candles for a specific instrument and time period."""
        params: dict[str, str | int] = {
            "granularity": granularity,
            "count": count,
            "price": price,
        }
        response = await self._make_request(f"instruments/{instrument}/candles", params)
        return cast(JSONType, response.json())
