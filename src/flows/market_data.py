import asyncio

from prefect import flow, task
from prefect.blocks.system import Secret

from src.ingest.core.manifest import PipelineManifest, ProcessorManifest, ProcessorType
from src.scripts.ingest.eodhd import run_eodhd_ingestion

# from src.scripts.ingest.oanda import run_oanda_ingestion
# from src.scripts.ingest.yahoo_finance import run_yahoo_finance_ingestion


@task
async def run_eodhd_ingestion_task() -> None:
    eodhd_api_key = await Secret.load("eodhd-api-key")
    gcp_bucket_name = await Secret.load("gcp-bucket-name")

    settings = {
        "api_key": eodhd_api_key.get(),
        "base_url": "https://eodhd.com/api/",
        "bucket_name": gcp_bucket_name.get(),
        "exchanges": ["EUFUND", "INDX"],
        "instruments": ["AAPL.US", "GB00B9876293.EUFUND", "GB00BG0QPQ07.EUFUND", "IXIC.INDX"],
        "macro_indicators": [
            "unemployment_total_percent",
            "inflation_consumer_prices_annual",
            "gdp_growth_annual",
        ],
        "macro_countries": ["GBR", "USA"],
    }

    manifest = PipelineManifest(
        name="eodhd_ingestion",
        processors=[
            ProcessorManifest(type=ProcessorType.EODHD_EXCHANGE, config={}),
            ProcessorManifest(type=ProcessorType.EODHD_EXCHANGE_SYMBOL, config={}),
            ProcessorManifest(type=ProcessorType.EODHD_INSTRUMENT, config={}),
            ProcessorManifest(type=ProcessorType.EODHD_MACRO, config={}),
            ProcessorManifest(type=ProcessorType.EODHD_ECONOMIC_EVENT, config={}),
        ],
        settings=settings,
    )
    await run_eodhd_ingestion(manifest)


# @task
# async def run_oanda_ingestion_task() -> None:
#     await run_oanda_ingestion()


# @task
# async def run_yahoo_finance_ingestion_task() -> None:
#     await run_yahoo_finance_ingestion()


@flow
async def run_market_data_ingestion() -> None:
    coros = [
        run_eodhd_ingestion_task(),
        # run_oanda_ingestion_task(),
        # run_yahoo_finance_ingestion_task(),
    ]
    await asyncio.gather(*coros)


if __name__ == "__main__":
    asyncio.run(run_market_data_ingestion())
