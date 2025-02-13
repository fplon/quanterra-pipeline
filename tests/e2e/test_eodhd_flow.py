import pytest

from src.orchestration.pipelines.ingest.eodhd_pipeline import eodhd_market_data_flow


@pytest.mark.asyncio
async def test_eodhd_flow() -> None:
    """Test the EODHD flow."""
    await eodhd_market_data_flow()
