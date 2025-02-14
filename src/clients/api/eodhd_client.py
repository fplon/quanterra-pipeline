from typing import Optional, cast

from clients.api.base_api_client import BaseAPIClient
from src.models.data.json_objects import JSONType  # TODO better implementation


class EODHDClient(BaseAPIClient):
    """EODHD API client for making HTTP requests."""

    def __init__(self, api_key: str, base_url: str) -> None:
        super().__init__(api_key, base_url)

    def _prepare_request_params(
        self, params: dict[str, str | int] | None = None
    ) -> dict[str, str | int]:
        """Add EODHD specific parameters."""
        prepared_params = params or {}
        prepared_params["api_token"] = self.api_key
        prepared_params["fmt"] = "json"
        return prepared_params

    def _get_headers(self) -> dict[str, str]:
        """EODHD doesn't use headers for authentication."""
        return {}

    async def get_exchanges(self) -> JSONType:
        """Get list of available exchanges."""
        response = await self._make_request("exchanges-list")
        return cast(JSONType, response.json())

    async def get_exchange_symbols(
        self, exchange: str, asset_type: Optional[str] = None, delisted: bool = False
    ) -> JSONType:
        """Get list of symbols for a specific exchange."""
        params: dict[str, str | int] = {}
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
        params: dict[str, str | int] = {}
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

    async def get_bulk_eod(self, exchange: str, date: str | None = None) -> JSONType:
        """Get bulk end-of-day data for an entire exchange."""
        params: dict[str, str | int] = {}
        if date:
            params["date"] = date
        response = await self._make_request(f"eod-bulk-last-day/{exchange}", params)
        return cast(JSONType, response.json())

    async def get_bulk_dividends(self, exchange: str, date: str | None = None) -> JSONType:
        """Get bulk dividends for an entire exchange."""
        params: dict[str, str | int] = {"type": "dividends"}
        if date:
            params["date"] = date
        response = await self._make_request(f"eod-bulk-last-day/{exchange}", params)
        return cast(JSONType, response.json())

    async def get_bulk_splits(self, exchange: str, date: str | None = None) -> JSONType:
        """Get bulk splits for an entire exchange."""
        params: dict[str, str | int] = {"type": "splits"}
        if date:
            params["date"] = date
        response = await self._make_request(f"eod-bulk-last-day/{exchange}", params)
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
        params: dict[str, str | int] = {
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
        params: dict[str, str | int] = {"indicator": indicator}
        response = await self._make_request(f"macro-indicator/{iso_code}", params)
        return cast(JSONType, response.json())

    async def get_news(self, instrument: str, exchange: str) -> JSONType:
        """Get news for a specific instrument."""
        params: dict[str, str | int] = {"s": f"{instrument}.{exchange}"}
        response = await self._make_request("news", params)
        return cast(JSONType, response.json())
