import pytest

from src.flows.interactive_investor import interactive_investor_transactions_flow


@pytest.mark.asyncio
async def test_interactive_investor_flow() -> None:
    await interactive_investor_transactions_flow(
        transactions_source_path=".notes/ii_transactions_sample.csv",
        portfolio_name="test_ii",
    )
