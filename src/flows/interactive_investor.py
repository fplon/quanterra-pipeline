from prefect import flow, get_run_logger
from prefect_gcp import GcpCredentials

from src.flows.models import Environment, InteractiveInvestorEnvironmentSettings
from src.ingest.data_sources.interactive_investor.client import InteractiveInvestorClient
from src.ingest.data_sources.interactive_investor.models import InteractiveInvestorConfig
from src.ingest.data_sources.interactive_investor.tasks import process_transactions


@flow(name="interactive_investor_transactions")
async def interactive_investor_transactions_flow(
    transactions_source_path: str,
    portfolio_name: str,
    env: str = "dev",
) -> None:
    """Flow for fetching Interactive Investor transactions."""
    logger = get_run_logger()
    env = Environment(env)
    logger.info(f"Starting Interactive Investor transactions flow in {env} environment")

    # Get credentials from Prefect blocks
    gcp_credentials = await GcpCredentials.load("quanterra-gcp-creds")

    # Create config
    env_settings = InteractiveInvestorEnvironmentSettings.get_settings(env)
    config = InteractiveInvestorConfig(
        bucket_name=env_settings.bucket_name,
        source_path=transactions_source_path,
        portfolio_name=portfolio_name,
        gcp_credentials=gcp_credentials.get_credentials_from_service_account(),
    )

    # Create a single client instance
    client = InteractiveInvestorClient(source_path=config.source_path)

    # Process transactions
    await process_transactions(config, client)
