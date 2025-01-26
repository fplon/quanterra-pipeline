from loguru import logger

from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.providers.eodhd.factory import EODHDProcessorFactory, ProcessorType
from src.ingest.providers.eodhd.models import EODHDConfig


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
            macro_indicators=settings.eodhd.macro_indicators["indicators"],
            macro_countries=settings.eodhd.macro_indicators["countries"],
        )

        # Initialise service factory
        factory = EODHDProcessorFactory()

        # Process exchange data
        exchange_processor = factory.create_processor(ProcessorType.EXCHANGE, eodhd_config)
        await exchange_processor.process()

        # Process exchange symbol data
        symbol_processor = factory.create_processor(ProcessorType.EXCHANGE_SYMBOL, eodhd_config)
        await symbol_processor.process()

        # Process instrument data
        instrument_processor = factory.create_processor(ProcessorType.INSTRUMENT, eodhd_config)
        await instrument_processor.process()

        # Process macroeconomic indicators data
        macro_processor = factory.create_processor(ProcessorType.MACRO, eodhd_config)
        await macro_processor.process()

        # Process economic events data
        econ_events_processor = factory.create_processor(ProcessorType.ECONOMIC_EVENT, eodhd_config)
        await econ_events_processor.process()

        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in EODHD ingestion")
        raise
