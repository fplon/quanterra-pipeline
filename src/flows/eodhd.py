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
        exchanges_bulk=env_settings.exchanges_bulk,
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

    # Fetch exchange symbols - defualt to config exchanges
    config_exchanges = config.exchanges + config.exchanges_bulk
    exchange_symbols = await fetch_exchange_symbols(config, client, config_exchanges or exchanges)

    # Fetch exchange bulk data - defualt to config bulk exchanges
    await fetch_exchange_bulk(config, client, config.exchanges_bulk or exchanges)

    # Fetch instrument-level data - defualt to config instruments otherwise filter exchange symbols
    await fetch_instruments(
        config,
        client,
        config.instruments or filter_exchange_symbols(exchange_symbols, config.exchanges),
    )

    # Fetch macro indicators
    await fetch_macro_indicators(config, client)

    # Fetch economic events
    await fetch_economic_events(config, client)


def filter_exchange_symbols(exchange_symbols: list[str], config_exchanges: list[str]) -> list[str]:
    """Filter exchange symbols to only include those that match the config exchanges."""
    filtered_exchange_symbols: list[str] = []
    for exchange in config_exchanges:
        for symbol in exchange_symbols:
            if f".{exchange}" in symbol:
                filtered_exchange_symbols.append(symbol)
    return filtered_exchange_symbols
