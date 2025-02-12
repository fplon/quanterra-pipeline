from datetime import datetime

from loguru import logger  # TODO replace with Prefect logging
from prefect import task

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.ingest.data_sources.hargreaves_lansdown.client import HargreavesLansdownClient
from src.ingest.data_sources.hargreaves_lansdown.models import (
    HargreavesLansdownClosedPosition,
    HargreavesLansdownConfig,
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
async def process_transactions(
    config: HargreavesLansdownConfig, client: HargreavesLansdownClient
) -> None:
    """Process and store Hargreaves Lansdown transaction data."""
    logger.info("Processing Hargreaves Lansdown transaction data")
    try:
        # TODO I don't like this setting and resetting the source path
        if config.transactions_source_path is None:
            raise ValueError("Transactions source path is not set")
        client.set_source_path(config.transactions_source_path)

        if not client.validate_file_type():
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        # TODO not sure is this is needed - dependency created though into StorageLocation
        preview_data = client.preview_file()
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
async def process_positions(
    config: HargreavesLansdownConfig, client: HargreavesLansdownClient
) -> None:
    """Process and store Hargreaves Lansdown positions data."""
    logger.info("Processing Hargreaves Lansdown positions data")
    try:
        # TODO I don't like this setting and resetting the source path - could also causa problems async if same client
        if config.positions_source_path is None:
            raise ValueError("Positions source path is not set")
        client.set_source_path(config.positions_source_path)

        if not client.validate_file_type():
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        preview_data = client.preview_file()
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
    config: HargreavesLansdownConfig, client: HargreavesLansdownClient
) -> None:
    """Process and store Hargreaves Lansdown closed positions data."""
    logger.info("Processing Hargreaves Lansdown closed positions data")
    try:
        # TODO I don't like this setting and resetting the source path - could also causa problems async if same client
        if config.closed_positions_source_path is None:
            raise ValueError("Closed positions source path is not set")

        client.set_source_path(config.closed_positions_source_path)
        if not client.validate_file_type():
            raise ValueError("Invalid Hargreaves Lansdown CSV file format")

        preview_data = client.preview_file()
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
