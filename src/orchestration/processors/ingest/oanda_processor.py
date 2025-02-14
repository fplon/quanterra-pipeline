import asyncio
from datetime import datetime

from loguru import logger
from prefect import task

from src.clients.api.oanda_client import OANDAClient
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import StorageLocation
from src.models.config.processor_settings import OANDAConfig
from src.models.data.oanda_models import CandlesData, InstrumentsData


@task(name="store_oanda_data", retries=3)
async def store_data(
    data: InstrumentsData | CandlesData,
    location: StorageLocation,
    config: OANDAConfig,
) -> None:
    """Store OANDA data in Google Cloud Storage."""
    storage_client = GCPStorageClient(credentials=config.gcp_credentials)
    storage_client.store_json_data(
        data=data.to_json(),
        bucket_name=location.bucket,
        blob_path=location.path,
        compress=True,
    )


# TODO rename fetch to something better, here and elsewhere
@task(name="fetch_oanda_instruments")
async def fetch_instruments(config: OANDAConfig, client: OANDAClient) -> list[str]:
    """Fetch and store available OANDA instruments."""
    logger.info("Fetching OANDA instruments data")

    try:
        raw_data = await client.get_instruments()
        data = InstrumentsData(
            data=raw_data,
            timestamp=datetime.now(),
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())

        await store_data(data, location, config)
        logger.success(f"Stored instruments data at: {location}")

        return data.get_instruments_list()
    except Exception:
        logger.exception("Error fetching instruments data")
        raise


async def process_instrument(
    client: OANDAClient,
    instrument: str,
    config: OANDAConfig,
) -> None:
    logger.info(f"Processing candles data for {instrument}")
    try:
        raw_data = await client.get_candles(
            instrument=instrument,
            granularity=config.granularity,
            count=config.count,
            price=config.price,
        )
        data = CandlesData(
            instrument=instrument,
            data=raw_data,
            timestamp=datetime.now(),
        )
        location = StorageLocation(
            bucket=config.bucket_name,
            path=data.get_storage_path(),
        )
        await store_data(data, location, config)
        logger.success(f"Stored candles data at: {location}")

    except Exception:
        logger.exception(f"Error processing candles data for {instrument}")


async def process_instruments(
    client: OANDAClient,
    instruments: list[str],
    config: OANDAConfig,
) -> None:
    """Process multiple instruments concurrently."""
    semaphore = asyncio.Semaphore(32)

    async def sem_task(instrument: str) -> None:
        async with semaphore:
            return await process_instrument(client, instrument, config)

    tasks = [sem_task(instrument) for instrument in instruments]
    await asyncio.gather(*tasks, return_exceptions=True)


@task(name="fetch_oanda_candles")
async def fetch_candles(
    config: OANDAConfig,
    client: OANDAClient,
    instruments: list[str] | None = None,
) -> None:
    """Fetch and store OANDA candles data."""
    logger.info("Fetching OANDA candles data")

    instruments_to_fetch = config.instruments or instruments or []
    await process_instruments(client, instruments_to_fetch, config)
