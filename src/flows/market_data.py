import asyncio

from prefect import flow, task
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials  # type: ignore

from src.ingest.core.manifest import PipelineManifest, ProcessorManifest, ProcessorType
from src.scripts.ingest.eodhd import run_eodhd_ingestion
from src.scripts.ingest.oanda import run_oanda_ingestion
from src.scripts.ingest.yahoo_finance import run_yahoo_finance_ingestion


@task
async def run_eodhd_ingestion_task() -> None:
    eodhd_api_key = await Secret.load("eodhd-api-key")
    gcp_bucket_name = await Secret.load("gcp-bucket-name")
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

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
        "gcp_credentials": gcp_credentials.get_credentials_from_service_account(),
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


@task
async def run_oanda_ingestion_task() -> None:
    oanda_api_key = await Secret.load("oanda-api-key")
    oanda_account_id = await Secret.load("oanda-account-id")
    gcp_bucket_name = await Secret.load("gcp-bucket-name")
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    settings = {
        "api_key": oanda_api_key.get(),
        "account_id": oanda_account_id.get(),
        "base_url": "https://api-fxtrade.oanda.com/v3/",
        "granularity": "D",
        "count": 50,
        "price": "MBA",
        "bucket_name": gcp_bucket_name.get(),
        "gcp_credentials": gcp_credentials.get_credentials_from_service_account(),
    }

    manifest = PipelineManifest(
        name="oanda_ingestion",
        processors=[
            ProcessorManifest(type=ProcessorType.OANDA_INSTRUMENTS, config={}),
            ProcessorManifest(type=ProcessorType.OANDA_CANDLES, config={}),
        ],
        settings=settings,
    )

    await run_oanda_ingestion(manifest)


@task
async def run_yahoo_finance_ingestion_task() -> None:
    gcp_bucket_name = await Secret.load("gcp-bucket-name")
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    settings = {
        "tickers": ["0P0000XYZ1.L", "0P000102MS.L", "0P0000KSPA.L"],
        "bucket_name": gcp_bucket_name.get(),
        "gcp_credentials": gcp_credentials.get_credentials_from_service_account(),
    }

    manifest = PipelineManifest(
        name="yahoo_finance_ingestion",
        processors=[
            ProcessorManifest(type=ProcessorType.YF_MARKET, config={}),
        ],
        settings=settings,
    )

    await run_yahoo_finance_ingestion(manifest)


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
