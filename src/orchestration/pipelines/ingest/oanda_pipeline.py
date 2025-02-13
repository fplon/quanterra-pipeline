from prefect import flow, get_run_logger
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials

from src.clients.oanda_client import OANDAClient
from src.models.config.pipeline_settings import Environment, OANDAEnvironmentSettings
from src.models.config.processor_settings import OANDAConfig
from src.orchestration.processors.ingest.oanda_processor import (
    fetch_candles,
    fetch_instruments,
)


@flow(name="oanda_market_data")
async def oanda_market_data_flow(env: str = "dev") -> None:
    """Flow for fetching OANDA market data."""
    logger = get_run_logger()
    env = Environment(env)
    logger.info(f"Starting OANDA market data flow in {env} environment")

    # Get credentials from Prefect blocks
    oanda_api_key = await Secret.load("oanda-api-key")
    oanda_account_id = await Secret.load("oanda-account-id")
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    # Create config
    env_settings = OANDAEnvironmentSettings.get_settings(env)
    config = OANDAConfig(
        api_key=oanda_api_key.get(),
        account_id=oanda_account_id.get(),
        base_url="https://api-fxtrade.oanda.com/v3/",
        instruments=env_settings.instruments,
        granularity=env_settings.granularity,
        count=env_settings.count,
        price="MBA",
        bucket_name=env_settings.bucket_name,
        gcp_credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    # Create a single client instance
    client = OANDAClient(
        api_key=config.api_key,
        base_url=config.base_url,
        account_id=config.account_id,
    )

    # Fetch instruments first
    instruments = await fetch_instruments(config, client)

    # Fetch candles for all available instruments
    await fetch_candles(config, client, instruments)
