from typing import cast

from src.clients.api.base_api_client import BaseAPIClient
from src.models.data.json_objects import JSONType  # TODO better implementation


class OANDAClient(BaseAPIClient):
    """OANDA API client for making HTTP requests."""

    def __init__(self, api_key: str, base_url: str, account_id: str) -> None:
        super().__init__(api_key, base_url)
        self.account_id = account_id

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
