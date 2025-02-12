import pytest

from src.flows.oanda import oanda_market_data_flow


@pytest.mark.asyncio
async def test_oanda_flow() -> None:
    await oanda_market_data_flow(env="dev")
