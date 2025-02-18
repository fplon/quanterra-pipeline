import pytest

from src.orchestration.pipelines.ingest.interactive_investor_pipeline import (
    interactive_investor_transactions_flow,
)


@pytest.mark.asyncio
async def test_interactive_investor_flow() -> None:
    await interactive_investor_transactions_flow(
        # NOTE requires these files to be uploaded to GCS
        transactions_source_path="transactions_cli_uploads/2025-02-18T10:18:31.218960/ii_transactions_sample.csv",
        portfolio_name="test_ii",
    )
