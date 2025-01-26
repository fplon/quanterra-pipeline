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
        )

        # Initialise service factory
        factory = EODHDServiceFactory()

        # Process exchange data
        service = factory.create_service(ServiceType.EXCHANGE, eodhd_config)
        await service.process()

        # Process exchange symbol data
        service = factory.create_service(ServiceType.EXCHANGE_SYMBOL, eodhd_config)
        await service.process()

        # Process instrument data
        data_types = ["eod", "dividends", "splits", "fundamentals", "news"]
        service = factory.create_service(ServiceType.INSTRUMENT, eodhd_config, data_types)
        await service.process()

        # Process economic data #TODO
        # service = factory.create_service(ServiceType.ECONOMIC, eodhd_config)
        # await service.process()

        logger.success("EODHD data ingestion completed successfully")
    except Exception:
        logger.exception("Fatal error in EODHD ingestion")
        raise
