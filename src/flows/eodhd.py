from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from src.flows.models import Environment, EODHDEnvironmentSettings
from src.ingest.data_sources.eodhd.client import EODHDClient
from src.ingest.data_sources.eodhd.models import EODHDConfig
from src.ingest.data_sources.eodhd.tasks import (
    fetch_economic_events,
    fetch_exchange_bulk,
    fetch_exchange_symbols,
    fetch_exchanges,
    fetch_instruments,
    fetch_macro_indicators,
)


@flow(name="eodhd_market_data")
async def eodhd_market_data_flow(
    env: str = "dev",
    run_exchages: bool = True,
    run_exchange_symbols: bool = True,
    run_exchange_bulk: bool = True,
    run_instruments: bool = True,
    run_macro_indicators: bool = True,
    run_economic_events: bool = True,
) -> None:
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
    if run_exchages:
        exchanges = await fetch_exchanges(config, client)
    else:
        exchanges = config.exchanges

    # Fetch exchange symbols
    if run_exchange_symbols:
        exchange_symbols = await fetch_exchange_symbols(config, client, exchanges)
    else:
        exchange_symbols = config.instruments

    # Fetch exchange bulk data
    if run_exchange_bulk:
        await fetch_exchange_bulk(config, client, exchanges)

    # Fetch instrument-level data
    if run_instruments:
        await fetch_instruments(config, client, exchange_symbols)

    # Fetch macro indicators
    if run_macro_indicators:
        await fetch_macro_indicators(config, client)

    # Fetch economic events
    if run_economic_events:
        await fetch_economic_events(config, client)
