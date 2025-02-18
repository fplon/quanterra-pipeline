import asyncio

from prefect import flow, get_run_logger
from prefect_gcp import GcpCredentials

from src.clients.yahoo_finance_client import YahooFinanceClient
from src.models.config.pipeline_settings import Environment, YahooFinanceEnvironmentSettings
from src.models.config.processor_settings import YahooFinanceConfig
from src.orchestration.processors.ingest.yahoo_finance_processor import (
    fetch_market_data,
    fetch_tickers,
)


@flow(name="yahoo_finance_market_data")
async def yahoo_finance_market_data_flow(env: str = "dev") -> None:
    """Flow for fetching Yahoo Finance market data."""
    logger = get_run_logger()
    env = Environment(env)
    logger.info(f"Starting Yahoo Finance market data flow in {env} environment")

    # Get credentials from Prefect blocks
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    # Create config
    env_settings = YahooFinanceEnvironmentSettings.get_settings(env)
    config = YahooFinanceConfig(
        bucket_name=env_settings.target_bucket_name,
        tickers=env_settings.tickers,
        gcp_credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    # Create a single client instance
    client = YahooFinanceClient()

    # Fetch ticker data and market data
    tasks = [
        fetch_tickers(config, client),
        fetch_market_data(config, client),
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
