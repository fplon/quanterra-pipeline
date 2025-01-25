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

        # TODO Exchange-level data processing - async?
        logger.info("Processing exchange data")
        try:
            with logger.catch(message="Failed to process exchange data"):
                location = await service.fetch_and_store_exchange_data()
                logger.success(f"Stored exchange data at: {location}")
        except Exception:
            logger.exception("Error processing exchange data")
            raise

        # TODO Exchange-symbol-level data processing - async?
        exchanges = settings.get_provider_exchanges("eodhd")

        logger.info(f"Processing exchange-symbol data for {len(exchanges)} exchanges")

        for exchange in exchanges:
            try:
                logger.info(f"Processing exchange-symbol data for {exchange}")
                with logger.catch(message="Failed to process exchange-symbol data"):
                    location = await service.fetch_and_store_exchange_symbol_data(exchange)
                    logger.success(f"Stored exchange-symbol data at: {location}")
            except Exception:
                logger.exception("Error processing exchange-symbol data")
                raise

        # TODO Instrument-level data processing - async?
        # Get all instrument pairs to process
        instrument_pairs = settings.get_provider_instruments("eodhd")

        # Data types to fetch
        data_types = ["eod", "dividends", "splits", "fundamentals", "news"]

        logger.info(
            f"Starting EODHD ingestion for {len(instrument_pairs)} instruments "
            f"across {len(data_types)} data types"
        )

        # Process each instrument
        for instrument, exchange in instrument_pairs:
            for data_type in data_types:
                try:
                    logger.info(f"Processing {data_type} data for {exchange}/{instrument}")
                    with logger.catch(
                        message=f"Failed to process {data_type} data for {exchange}/{instrument}"
                    ):
                        location = await service.fetch_and_store_data(
                            instrument=instrument, exchange=exchange, data_type=data_type
                        )
                        logger.success(
                            f"Stored {data_type} data for {exchange}/{instrument} at: {location}"
                        )

                except Exception:
                    logger.exception(
                        f"Error processing {data_type} data for {exchange}/{instrument}"
                    )
                    continue

        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in main")
        raise
