import asyncio

from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from src.flows.models import Environment, EODHDEnvironmentSettings
from src.ingest.data_sources.eodhd.client import EODHDClient
from src.ingest.data_sources.eodhd.models import EODHDConfig
from src.ingest.data_sources.eodhd.tasks import (
    fetch_economic_events,
    fetch_exchange_symbols,
    fetch_exchanges,
    fetch_instruments,
    fetch_macro_indicators,
)


@flow(name="eodhd_market_data")
async def eodhd_market_data_flow(env: str = "dev") -> None:
    """Flow for fetching EODHD market data."""
    logger = get_run_logger()
    env = Environment(env)
    logger.info(f"Starting EODHD market data flow in {env} environment")

    # Get credentials from Prefect blocks
    eodhd_api_key = await Secret.load("eodhd-api-key")
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    # Create config
    env_settings = EODHDEnvironmentSettings.get_settings(env)
    config = EODHDConfig(
        api_key=eodhd_api_key.get(),
        base_url="https://eodhd.com/api/",
        exchanges=env_settings.exchanges,
        instruments=env_settings.instruments,
        macro_indicators=env_settings.macro_indicators,
        macro_countries=env_settings.macro_countries,
        bucket_name=env_settings.bucket_name,
        gcp_credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    # Create a single client instance
    client = EODHDClient(
        api_key=config.api_key,
        base_url=config.base_url,
    )

    # Fetch exchanges
    exchanges = await fetch_exchanges(config, client)

    # Fetch exchange symbols
    exchange_symbols = await fetch_exchange_symbols(config, client, exchanges)

    # Fetch instrument-level data
    await fetch_instruments(config, client, exchange_symbols)

    # Fetch macro-level data and economic events
    econ_tasks = [
        fetch_macro_indicators(config, client),
        fetch_economic_events(config, client),
    ]
    await asyncio.gather(*econ_tasks, return_exceptions=True)
