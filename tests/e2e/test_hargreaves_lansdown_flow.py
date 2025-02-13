import pytest

from src.orchestration.pipelines.ingest.hargreaves_lansdown_pipeline import (
    hargreaves_lansdown_transactions_flow,
)


@pytest.mark.asyncio
async def test_hargreaves_lansdown_transactions_flow() -> None:
    await hargreaves_lansdown_transactions_flow(
        transactions_source_path=".notes/hl_transactions_sample.csv",
        positions_source_path=".notes/hl_positions_sample.csv",
        closed_positions_source_path=".notes/hl_closed_positions_sample.csv",
        portfolio_name="test_hl",
    )
