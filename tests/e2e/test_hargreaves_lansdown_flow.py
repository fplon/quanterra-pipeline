import pytest

from src.orchestration.pipelines.ingest.hargreaves_lansdown_pipeline import (
    hargreaves_lansdown_transactions_flow,
)


@pytest.mark.asyncio
async def test_hargreaves_lansdown_transactions_flow() -> None:
    await hargreaves_lansdown_transactions_flow(
        # NOTE requires these files to be uploaded to GCS
        transactions_source_path="transactions_cli_uploads/2025-02-17T14:57:34.578929/hl_transactions_sample.csv",
        positions_source_path="transactions_cli_uploads/2025-02-17T14:57:34.718748/hl_positions_sample.csv",
        closed_positions_source_path="transactions_cli_uploads/2025-02-17T14:57:34.805892/hl_closed_positions_sample.csv",
        portfolio_name="test_hl",
    )
