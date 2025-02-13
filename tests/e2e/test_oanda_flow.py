import pytest

from src.orchestration.pipelines.ingest.oanda_pipeline import oanda_market_data_flow


@pytest.mark.asyncio
async def test_oanda_flow() -> None:
    await oanda_market_data_flow(env="dev")
