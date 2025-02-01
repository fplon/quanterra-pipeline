from loguru import logger
from prefect import flow, task

from src.scripts.ingest.eodhd import run_eodhd_ingestion
from src.scripts.ingest.hargreaves_lansdown import run_hargreaves_lansdown_ingestion
from src.scripts.ingest.interactive_investor import run_interactive_investor_ingestion
from src.scripts.ingest.oanda import run_oanda_ingestion
from src.scripts.ingest.yahoo_finance import run_yahoo_finance_ingestion


@task(name="ingest_eodhd", log_prints=True, retries=3, retry_delay_seconds=60)
async def ingest_eodhd() -> None:
    """Task to ingest EODHD data."""
    try:
        await run_eodhd_ingestion()
    except Exception as e:
        logger.error(f"EODHD ingestion failed: {str(e)}")
        raise


@task(name="ingest_hargreaves_lansdown", log_prints=True, retries=3, retry_delay_seconds=60)
async def ingest_hargreaves_lansdown() -> None:
    """Task to ingest Hargreaves Lansdown data."""
    try:
        await run_hargreaves_lansdown_ingestion()
    except Exception as e:
        logger.error(f"Hargreaves Lansdown ingestion failed: {str(e)}")
        raise


@task(name="ingest_interactive_investor", log_prints=True, retries=3, retry_delay_seconds=60)
async def ingest_interactive_investor() -> None:
    """Task to ingest Interactive Investor data."""
    try:
        await run_interactive_investor_ingestion()
    except Exception as e:
        logger.error(f"Interactive Investor ingestion failed: {str(e)}")
        raise


@task(name="ingest_oanda", log_prints=True, retries=3, retry_delay_seconds=60)
async def ingest_oanda() -> None:
    """Task to ingest OANDA data."""
    try:
        await run_oanda_ingestion()
    except Exception as e:
        logger.error(f"OANDA ingestion failed: {str(e)}")
        raise


@task(name="ingest_yahoo_finance", log_prints=True, retries=3, retry_delay_seconds=60)
async def ingest_yahoo_finance() -> None:
    """Task to ingest Yahoo Finance data."""
    try:
        await run_yahoo_finance_ingestion()
    except Exception as e:
        logger.error(f"Yahoo Finance ingestion failed: {str(e)}")
        raise


@flow(name="market_data_ingestion")
async def market_data_ingestion_flow() -> None:
    """Main flow for market data ingestion."""
    # Run market data ingestion tasks in parallel
    await ingest_eodhd()
    await ingest_oanda()
    await ingest_yahoo_finance()

    # Run broker data ingestion tasks in parallel
    await ingest_hargreaves_lansdown()
    await ingest_interactive_investor()


if __name__ == "__main__":
    import asyncio

    asyncio.run(market_data_ingestion_flow())
