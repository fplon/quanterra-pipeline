import asyncio

from src.scripts.ingest.yahoo_finance import run_yahoo_finance_ingestion


def test_yahoo_finance_ingestion_script() -> None:
    asyncio.run(run_yahoo_finance_ingestion())
    assert True
