from datetime import datetime

from loguru import logger  # TODO replace with Prefect logging
from prefect import task

from clients.file.base_csv_client import BaseCSVFileClient
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import StorageLocation
from src.models.config.processor_settings import HargreavesLansdownConfig
from src.models.data.hargreaves_lansdown_models import (
    HargreavesLansdownClosedPosition,
    HargreavesLansdownPosition,
    HargreavesLansdownTransaction,
)


@task(name="store_hargreaves_lansdown_data", retries=3)
async def store_data(
    location: StorageLocation,
    config: HargreavesLansdownConfig,
    source_path: str,
) -> None:
    """Store Hargreaves Lansdown data in Google Cloud Storage."""
    storage_client = GCPStorageClient(credentials=config.gcp_credentials)
    storage_client.store_csv_file(
        source_path=source_path,  # slightly different to II
        bucket_name=location.bucket,
        blob_path=location.path,
        compress=True,
    )


@task(name="process_hargreaves_lansdown_transactions")
async def process_transactions(config: HargreavesLansdownConfig, client: BaseCSVFileClient) -> None:
    """Process and store Hargreaves Lansdown transaction data."""
    logger.info("Processing Hargreaves Lansdown transaction data")
    try:
        if not client.validate_file_type(config.transactions_source_path):
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        preview_data = client.preview_file(config.transactions_source_path)
        data = HargreavesLansdownTransaction(
            data=preview_data,
            portfolio_name=config.portfolio_name,
            timestamp=datetime.now(),
        )

        location = StorageLocation(
            bucket=config.bucket_name,
            path=data.get_storage_path(),
        )

        await store_data(location, config, config.transactions_source_path)
        logger.success(f"Stored Hargreaves Lansdown transaction data at: {location}")
    except Exception:
        logger.exception("Error processing Hargreaves Lansdown transaction data")
        raise


@task(name="process_hargreaves_lansdown_positions")
async def process_positions(config: HargreavesLansdownConfig, client: BaseCSVFileClient) -> None:
    """Process and store Hargreaves Lansdown positions data."""
    logger.info("Processing Hargreaves Lansdown positions data")
    try:
        if not client.validate_file_type(config.positions_source_path):
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        preview_data = client.preview_file(config.positions_source_path)
        data = HargreavesLansdownPosition(
            data=preview_data,
            portfolio_name=config.portfolio_name,
            timestamp=datetime.now(),
        )

        location = StorageLocation(
            bucket=config.bucket_name,
            path=data.get_storage_path(),
        )

        await store_data(location, config, config.positions_source_path)
        logger.success(f"Stored Hargreaves Lansdown positions data at: {location}")
    except Exception:
        logger.exception("Error processing Hargreaves Lansdown positions data")
        raise


@task(name="process_hargreaves_lansdown_closed_positions")
async def process_closed_positions(
    config: HargreavesLansdownConfig, client: BaseCSVFileClient
) -> None:
    """Process and store Hargreaves Lansdown closed positions data."""
    logger.info("Processing Hargreaves Lansdown closed positions data")
    try:
        if not client.validate_file_type(config.closed_positions_source_path):
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        preview_data = client.preview_file(config.closed_positions_source_path)
        data = HargreavesLansdownClosedPosition(
            data=preview_data,
            portfolio_name=config.portfolio_name,
            timestamp=datetime.now(),
        )

        location = StorageLocation(
            bucket=config.bucket_name,
            path=data.get_storage_path(),
        )

        await store_data(location, config, config.closed_positions_source_path)
        logger.success(f"Stored Hargreaves Lansdown closed positions data at: {location}")
    except Exception:
        logger.exception("Error processing Hargreaves Lansdown closed positions data")
        raise
