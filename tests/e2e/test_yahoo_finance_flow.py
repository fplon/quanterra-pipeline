import pytest

from src.orchestration.pipelines.ingest.yahoo_finance_pipeline import yahoo_finance_market_data_flow


@pytest.mark.asyncio
async def test_yahoo_finance_flow() -> None:
    """Test the Yahoo Finance flow."""
    await yahoo_finance_market_data_flow()
