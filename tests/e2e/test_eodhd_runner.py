import asyncio

from src.ingest.runners.eodhd import main


def test_eodhd_runner() -> None:
    asyncio.run(main())
    assert True
