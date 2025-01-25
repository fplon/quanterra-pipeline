from loguru import logger

from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.providers.eodhd.models import EODHDConfig
from src.ingest.providers.eodhd.service import EODHDService


async def run_eodhd_ingestion() -> None:
    """Run EODHD data ingestion for all configured instruments."""
    try:
        # Initialise logger for EODHD component
        setup_logger("eodhd")
        logger.info("Starting EODHD data ingestion")

        # Get settings
        settings = get_settings()

        # TODO why not just use settings object directly?
        # Create EODHD config
        eodhd_config = EODHDConfig(
            api_key=settings.eodhd.api_key,
            base_url=settings.eodhd.base_url,
            bucket_name=settings.gcp.bucket_name,
            exchanges=settings.eodhd.exchanges,
            instruments=settings.eodhd.instruments,
        )

        # Create service
        service = EODHDService(config=eodhd_config)

        # TODO this is too unique to prices and also not a good place to have this
        # Get all instrument pairs to process
        instrument_pairs = settings.get_provider_instruments("eodhd")

        logger.info(f"Starting EODHD ingestion for {len(instrument_pairs)} instruments")

        # Process each instrument
        for exchange, instrument in instrument_pairs:
            try:
                logger.info(f"Processing {exchange}/{instrument}")
                with logger.catch(message=f"Failed to process {exchange}/{instrument}"):
                    location = await service.fetch_and_store_eod_data(
                        instrument=instrument, exchange=exchange
                    )
                    logger.success(f"Stored {exchange}/{instrument} at: {location}")

            except Exception:
                logger.exception(f"Error processing {exchange}/{instrument}")
                continue

        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in main")
        raise
