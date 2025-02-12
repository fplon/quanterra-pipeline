import pytest

from src.flows.eodhd import eodhd_market_data_flow


@pytest.mark.asyncio
async def test_eodhd_flow() -> None:
    """Test the EODHD flow."""
    await eodhd_market_data_flow(run_instruments=False)
