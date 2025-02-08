import pytest

from src.flows.market_data import run_market_data_ingestion


@pytest.mark.asyncio
async def test_market_data_flow() -> None:
    await run_market_data_ingestion()
