import asyncio

from prefect import flow, get_run_logger
from prefect_gcp import GcpCredentials

from src.clients.file.base_csv_client import BaseCSVFileClient
from src.models.config.pipeline_settings import Environment, SimpleEnvironmentSettings
from src.models.config.processor_settings import HargreavesLansdownConfig
from src.orchestration.processors.ingest.hargreaves_lansdown_processor import (
    process_closed_positions,
    process_positions,
    process_transactions,
)


@flow(name="hargreaves_lansdown_transactions")
async def hargreaves_lansdown_transactions_flow(
    transactions_source_path: str,
    positions_source_path: str,
    closed_positions_source_path: str,
    portfolio_name: str,
    env: str = "dev",
) -> None:
    """Flow for fetching Hargreaves Lansdown transactions."""
    logger = get_run_logger()
    env = Environment(env)
    logger.info(f"Starting Hargreaves Lansdown transactions flow in {env} environment")

    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    env_settings = SimpleEnvironmentSettings.get_settings(env)
    config = HargreavesLansdownConfig(
        bucket_name=env_settings.bucket_name,
        transactions_source_path=transactions_source_path,
        positions_source_path=positions_source_path,
        closed_positions_source_path=closed_positions_source_path,
        portfolio_name=portfolio_name,
        gcp_credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    client = BaseCSVFileClient()

    # Process transactions
    process_tasks = [
        process_transactions(config, client),
        process_positions(config, client),
        process_closed_positions(config, client),
    ]
    await asyncio.gather(*process_tasks)
