from loguru import logger

from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.data_sources.oanda.factory import OANDAProcessorFactory, ProcessorType
from src.ingest.data_sources.oanda.models import OANDAConfig


async def run_oanda_ingestion() -> None:
    """Run OANDA data ingestion for all configured instruments."""
    try:
        # Initialise logger for OANDA component
        setup_logger("oanda")
        logger.info("Starting OANDA data ingestion")

        # Get settings
        settings = get_settings()

        # Create OANDA config
        oanda_config = OANDAConfig(
            api_key=settings.oanda.api_key,
            base_url=settings.oanda.base_url,
            bucket_name=settings.gcp.bucket_name,
            account_id=settings.oanda.account_id,
            instruments=settings.oanda.instruments,
            granularity=settings.oanda.granularity,
            count=settings.oanda.count,
            price=settings.oanda.price,
        )

        # Initialise processor factory
        factory = OANDAProcessorFactory()

        # Process instruments data
        instruments_processor = factory.create_processor(ProcessorType.INSTRUMENTS, oanda_config)
        await instruments_processor.process()

        if not oanda_config.instruments:
            logger.warning("No instruments configured for OANDA data ingestion")
            # TODO - parse instruments from output of instruments processor

        # Process candles data
        candles_processor = factory.create_processor(ProcessorType.CANDLES, oanda_config)
        await candles_processor.process()

        logger.success("OANDA data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in OANDA ingestion")
        raise
