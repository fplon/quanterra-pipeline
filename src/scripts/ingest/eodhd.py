from loguru import logger

from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.providers.eodhd.factory import EODHDServiceFactory, ServiceType
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
        factory = EODHDServiceFactory()

        # Process exchange data
        exchange_service = factory.create_service(ServiceType.EXCHANGE, eodhd_config)
        await exchange_service.process()

        # Process exchange symbol data
        symbol_service = factory.create_service(ServiceType.EXCHANGE_SYMBOL, eodhd_config)
        await symbol_service.process()

        # Process instrument data
        instrument_service = factory.create_service(ServiceType.INSTRUMENT, eodhd_config)
        await instrument_service.process()

        # Process macroeconomic indicators data
        macro_service = factory.create_service(ServiceType.MACRO, eodhd_config)
        await macro_service.process()

        # Process economic events data
        econ_events_service = factory.create_service(ServiceType.ECONOMIC_EVENT, eodhd_config)
        await econ_events_service.process()

        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in EODHD ingestion")
        raise
