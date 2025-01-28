import asyncio

from src.scripts.ingest.oanda import run_oanda_ingestion


def test_oanda_ingestion_script() -> None:
    asyncio.run(run_oanda_ingestion())
    assert True
