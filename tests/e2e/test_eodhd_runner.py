import asyncio

from src.scripts.ingest.eodhd import run_eodhd_ingestion


def test_eodhd_ingestion_script() -> None:
    asyncio.run(run_eodhd_ingestion())
    assert True
