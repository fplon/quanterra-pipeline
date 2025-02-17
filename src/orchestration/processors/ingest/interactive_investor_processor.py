from datetime import datetime

from loguru import logger
from prefect import task

from src.clients.file.google_cloud_file_client import GoogleCloudFileClient
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import StorageLocation
from src.models.config.processor_settings import InteractiveInvestorConfig
from src.models.data.interactive_investor_models import (
    InteractiveInvestorTransaction,
)


@task(name="store_interactive_investor_data", retries=3)
async def store_data(
    location: StorageLocation,
    config: InteractiveInvestorConfig,
) -> None:
    """Store Interactive Investor data in Google Cloud Storage."""
    storage_client = GCPStorageClient(credentials=config.gcp_credentials)
    # TODO better transfer bucket to bucket
    storage_client.store_csv_file_from_blob(
        source_bucket_name=location.bucket,  # TODO make discinct in case different buckets used
        source_blob_path=config.source_path,
        target_bucket_name=location.bucket,
        target_blob_path=location.path,
        compress=True,
    )


@task(name="process_interactive_investor_transactions")
async def process_transactions(
    config: InteractiveInvestorConfig, client: GoogleCloudFileClient
) -> None:
    """Process and store Interactive Investor transaction data."""
    logger.info("Processing Interactive Investor transaction data")
    try:
        # Validate file format
        if not client.validate_file_type(config.source_path):
            raise ValueError("Invalid Interactive Investor CSV file format")

        # Preview data for validation
        # TODO not sure is this is needed - dependency created though into StorageLocation
        preview_data = client.preview_file(config.source_path)
        data = InteractiveInvestorTransaction(
            data=preview_data,
            portfolio_name=config.portfolio_name,
            timestamp=datetime.now(),
        )

        # Get storage location
        location = StorageLocation(
            bucket=config.bucket_name,
            path=data.get_storage_path(),
        )

        # Store the CSV file with compression
        await store_data(location, config)

        logger.success(f"Stored Interactive Investor transaction data at: {location}")
    except Exception:
        logger.exception("Error processing Interactive Investor transaction data")
        raise
