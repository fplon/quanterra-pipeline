import asyncio

from prefect import flow, task

from src.scripts.ingest.eodhd import run_eodhd_ingestion
from src.scripts.ingest.oanda import run_oanda_ingestion
from src.scripts.ingest.yahoo_finance import run_yahoo_finance_ingestion


@task
async def run_eodhd_ingestion_task() -> None:
    await run_eodhd_ingestion()


@task
async def run_oanda_ingestion_task() -> None:
    await run_oanda_ingestion()


@task
async def run_yahoo_finance_ingestion_task() -> None:
    await run_yahoo_finance_ingestion()


@flow
async def run_market_data_ingestion() -> None:
    coros = [
        run_eodhd_ingestion_task(),
        run_oanda_ingestion_task(),
        run_yahoo_finance_ingestion_task(),
    ]
    await asyncio.gather(*coros)


if __name__ == "__main__":
    asyncio.run(run_market_data_ingestion())
