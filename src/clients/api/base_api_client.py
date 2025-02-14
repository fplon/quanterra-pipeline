import httpx
from httpx import Response
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from src.utils.utilities import log_retry_attempt


class BaseAPIClient:
    """Base API client for making HTTP requests."""

    def __init__(self, api_key: str, base_url: str) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")

    def _prepare_request_params(
        self, params: dict[str, str | int] | None = None
    ) -> dict[str, str | int]:
        """Prepare request parameters. Override in child classes if needed."""
        return params or {}

    def _get_headers(self) -> dict[str, str]:
        """Get headers for API requests. Override in child classes if needed."""
        return {"Authorization": f"Bearer {self.api_key}"}

    @retry(
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPError)),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        before_sleep=log_retry_attempt,
    )
    async def _make_request(
        self, endpoint: str, params: dict[str, str | int] | None = None
    ) -> Response:
        """Make HTTP request to API and return raw response."""
        prepared_params = self._prepare_request_params(params)
        url = f"{self.base_url}/{endpoint}"

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=prepared_params, headers=self._get_headers())
            response.raise_for_status()
            return response
