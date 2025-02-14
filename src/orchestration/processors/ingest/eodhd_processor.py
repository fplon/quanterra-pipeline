import asyncio
from datetime import datetime

from loguru import logger
from prefect import task

from clients.api.eodhd_client import EODHDClient
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import StorageLocation
from src.models.config.processor_settings import EODHDConfig
from src.models.data.eodhd_models import (
    EconomicEventData,
    ExchangeBulkData,
    ExchangeData,
    ExchangeSymbolData,
    InstrumentData,
    MacroData,
)

storage_data_type = (
    ExchangeData
    | ExchangeSymbolData
    | InstrumentData
    | MacroData
    | EconomicEventData
    | ExchangeBulkData
)


@task(name="store_eodhd_data", retries=3)
async def store_eodhd_data(
    data: storage_data_type, location: StorageLocation, config: EODHDConfig
) -> None:
    """Store EODHD data in Google Cloud Storage."""
    storage_client = GCPStorageClient(credentials=config.gcp_credentials)
    storage_client.store_json_data(
        data=data.to_json(),
        bucket_name=location.bucket,
        blob_path=location.path,
        compress=True,
    )


@task(name="fetch_eodhd_exchanges")
async def fetch_exchanges(config: EODHDConfig, client: EODHDClient) -> list[str]:
    """Fetch and store EODHD exchanges."""
    logger.info("Fetching EODHD exchanges data")

    try:
        raw_data = await client.get_exchanges()
        data = ExchangeData(
            data=raw_data,
            timestamp=datetime.now(),
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())

        # TODO handle this returning [] or failing
        await store_eodhd_data(data, location, config)
        logger.success(f"Stored exchanges data at: {location}")

        return data.get_exchanges_list()
    except Exception:
        logger.exception("Error fetching exchanges data")
        raise


@task(name="fetch_eodhd_economic_events")
async def fetch_economic_events(config: EODHDConfig, client: EODHDClient) -> None:
    """Fetch and store EODHD economic events."""
    logger.info("Fetching EODHD economic events data")

    try:
        raw_data = await client.get_economic_events()
        data = EconomicEventData(
            data=raw_data,
            timestamp=datetime.now(),
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())

        await store_eodhd_data(data, location, config)
        logger.success(f"Stored economic events data at: {location}")

    except Exception:
        logger.exception("Error fetching economic events data")
        raise


async def process_exchange_symbol(
    config: EODHDConfig, client: EODHDClient, exchange: str
) -> list[str]:
    """Process EODHD exchange symbol data."""
    logger.info(f"Processing exchange symbol data for {exchange}")
    try:
        raw_data = await client.get_exchange_symbols(exchange)
        data = ExchangeSymbolData(
            data=raw_data,
            timestamp=datetime.now(),
            exchange=exchange,
            data_type="exchange-symbol-list",
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())
        await store_eodhd_data(data, location, config)
        logger.success(f"Stored exchange symbol data at: {location}")

        return data.get_exchange_symbols_list()

    except Exception:
        logger.exception(f"Error processing exchange symbol data for {exchange}")
        return []  # TODO handle this better


async def process_exchange_symbols(
    config: EODHDConfig, client: EODHDClient, exchanges: list[str]
) -> list[str]:
    """Process multiple exchange symbols concurrently."""
    tasks = [process_exchange_symbol(config, client, exchange) for exchange in exchanges]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Flatten the list of lists and filter out any exceptions
    all_symbols = []
    for result in results:
        if isinstance(result, list):
            all_symbols.extend(result)
        else:
            logger.error(f"Failed to process exchange symbols: {result}")

    return all_symbols


@task(name="fetch_eodhd_exchange_symbols")
async def fetch_exchange_symbols(
    config: EODHDConfig,
    client: EODHDClient,
    exchanges: list[str],
) -> list[str]:
    """Fetch and store EODHD exchange symbols."""
    logger.info("Fetching EODHD exchange symbols data")

    return await process_exchange_symbols(config, client, exchanges)


async def process_exchange_bulk(
    config: EODHDConfig, client: EODHDClient, exchange: str, data_type: str, date: str | None = None
) -> None:
    """Process EODHD exchange bulk data."""
    logger.info(f"Processing exchange bulk data for {exchange} {data_type}")
    # TODO prevent INDX and EUFUND from bulk dividends and splits
    # TODO run 2-3 days at a time
    try:
        if data_type == "bulk_eod":
            raw_data = await client.get_bulk_eod(exchange, date)
        elif data_type == "bulk_dividends":
            raw_data = await client.get_bulk_dividends(exchange, date)
        elif data_type == "bulk_splits":
            raw_data = await client.get_bulk_splits(exchange, date)
        else:
            raise ValueError(f"Invalid data type: {data_type}")
        data = ExchangeBulkData(
            data=raw_data,
            timestamp=datetime.now(),
            data_type=data_type,
            exchange=exchange,
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())
        await store_eodhd_data(data, location, config)
        logger.success(f"Stored exchange bulk data at: {location}")

    except Exception:
        logger.exception(f"Error processing exchange bulk {data_type} data for {exchange}")


async def process_exchange_bulk_data(
    config: EODHDConfig, client: EODHDClient, exchanges: list[str]
) -> None:
    """Process multiple exchange bulk data concurrently."""
    data_types = ["bulk_eod", "bulk_dividends", "bulk_splits"]

    semaphore = asyncio.Semaphore(8)  # Limit to concurrent requests

    async def sem_task(exchange: str, data_type: str) -> None:
        async with semaphore:
            return await process_exchange_bulk(config, client, exchange, data_type)

    tasks = [sem_task(exchange, data_type) for exchange in exchanges for data_type in data_types]
    await asyncio.gather(*tasks, return_exceptions=True)


@task(name="fetch_eodhd_exchange_bulk")
async def fetch_exchange_bulk(
    config: EODHDConfig,
    client: EODHDClient,
    exchanges: list[str],
) -> None:
    """Fetch and store EODHD exchange bulk data - eod, dividends, splits."""
    logger.info("Fetching EODHD exchange bulk data")

    await process_exchange_bulk_data(config, client, exchanges)


async def process_instrument(
    config: EODHDConfig, client: EODHDClient, instrument: str, endpoint: str
) -> None:
    """Process EODHD instrument data."""
    logger.info(f"Processing instrument {endpoint} data for {instrument}")
    try:
        code, exchange = instrument.split(".", 1)
        # TODO map instead of if block
        if endpoint == "dividends":
            raw_data = await client.get_dividends(code, exchange)
        elif endpoint == "splits":
            raw_data = await client.get_splits(code, exchange)
        elif endpoint == "eod":
            raw_data = await client.get_eod_data(code, exchange)
        elif endpoint == "fundamentals":
            raw_data = await client.get_fundamentals(code, exchange)
        elif endpoint == "news":
            raw_data = await client.get_news(code, exchange)
        else:
            raise ValueError(f"Invalid endpoint: {endpoint}")

        data = InstrumentData(
            data=raw_data,
            timestamp=datetime.now(),
            code=code,
            exchange=exchange,
            data_type=endpoint,
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())
        await store_eodhd_data(data, location, config)
        logger.success(f"Stored instrument data at: {location}")

    except Exception:
        logger.exception(f"Error processing instrument {endpoint} data for {instrument}")


async def process_instruments(
    config: EODHDConfig, client: EODHDClient, instruments: list[str]
) -> None:
    """Process multiple instruments concurrently with a limit on concurrency."""
    endpoints = ["dividends", "splits", "eod", "fundamentals", "news"]
    semaphore = asyncio.Semaphore(40)  # Limit to concurrent requests

    async def sem_task(instrument: str, endpoint: str) -> None:
        async with semaphore:
            return await process_instrument(config, client, instrument, endpoint)

    tasks = [sem_task(instrument, endpoint) for instrument in instruments for endpoint in endpoints]
    await asyncio.gather(*tasks, return_exceptions=True)


@task(name="fetch_eodhd_instruments")
async def fetch_instruments(
    config: EODHDConfig,
    client: EODHDClient,
    instruments: list[str],
) -> None:
    """Fetch and store EODHD instruments."""
    logger.info("Fetching EODHD instruments data")

    await process_instruments(config, client, instruments)


async def process_macro_indicator(
    config: EODHDConfig, client: EODHDClient, iso_code: str, indicator: str
) -> None:
    """Process single EODHD macro indicator data."""
    logger.info(f"Processing macro indicator data for {iso_code} {indicator}")

    try:
        raw_data = await client.get_macro_indicator(iso_code, indicator)
        data = MacroData(
            data=raw_data,
            timestamp=datetime.now(),
            iso_code=iso_code,
            indicator=indicator,
        )
        location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())
        await store_eodhd_data(data, location, config)
        logger.success(f"Stored macro indicator data at: {location}")

    except Exception:
        logger.exception(f"Error processing macro indicator data for {indicator}")


async def process_macro_indicators(
    config: EODHDConfig, client: EODHDClient, indicators: list[str], iso_codes: list[str]
) -> None:
    """Process multiple macro indicators concurrently."""
    semaphore = asyncio.Semaphore(40)  # Limit to concurrent requests

    async def sem_task(iso_code: str, indicator: str) -> None:
        async with semaphore:
            return await process_macro_indicator(config, client, iso_code, indicator)

    tasks = [sem_task(iso_code, indicator) for indicator in indicators for iso_code in iso_codes]
    await asyncio.gather(*tasks, return_exceptions=True)


@task(name="fetch_eodhd_macro_indicators")
async def fetch_macro_indicators(
    config: EODHDConfig,
    client: EODHDClient,
    indicators: list[str] | None = None,
    iso_codes: list[str] | None = None,
) -> None:
    """Fetch and store EODHD macro indicators."""
    logger.info("Fetching EODHD macro indicators data")

    indicators_to_fetch = config.macro_indicators or indicators or []
    iso_codes_to_fetch = config.macro_countries or iso_codes or []

    await process_macro_indicators(config, client, indicators_to_fetch, iso_codes_to_fetch)
