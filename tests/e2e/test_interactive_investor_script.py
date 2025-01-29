import asyncio

from src.scripts.ingest.interactive_investor import run_interactive_investor_ingestion


def test_interactive_investor_ingestion_script() -> None:
    asyncio.run(run_interactive_investor_ingestion())
    assert True
