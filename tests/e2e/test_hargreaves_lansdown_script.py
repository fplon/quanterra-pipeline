import asyncio

from src.scripts.ingest.hargreaves_lansdown import run_hargreaves_lansdown_ingestion


def test_hargreaves_lansdown_script() -> None:
    asyncio.run(run_hargreaves_lansdown_ingestion())
    assert True
