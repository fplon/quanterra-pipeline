import pytest

from src.orchestration.pipelines.ingest.interactive_investor_pipeline import (
    interactive_investor_transactions_flow,
)


@pytest.mark.asyncio
async def test_interactive_investor_flow() -> None:
    await interactive_investor_transactions_flow(
        transactions_source_path="temp_uploads/2025-02-16T20:26:32.568715/ii_transactions_sample.csv",
        portfolio_name="test_ii",
    )
