from loguru import logger

from ..config.settings import get_settings
from ..providers.eodhd.models import EODHDConfig
from ..providers.eodhd.service import EODHDService

# Configure loguru
logger.remove()  # Remove default handler
logger.add(
    "logs/eodhd_{time}.log",
    rotation="1 day",
    retention="1 week",
    compression="zip",
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
)
logger.add(
    lambda msg: print(msg),
    colorize=True,
    format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
)


async def run_eodhd_ingestion() -> None:
    """Run EODHD data ingestion for all configured instruments."""
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


async def main() -> None:
    """Main entry point."""
    try:
        logger.info("Starting EODHD data ingestion")
        await run_eodhd_ingestion()
        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in main")
        raise
