from loguru import logger

from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.data_sources.yahoo_finance.models import YahooFinanceConfig
from src.ingest.data_sources.yahoo_finance.processors import YahooFinanceProcessor


async def run_yahoo_finance_ingestion() -> None:
    """Run Yahoo Finance data ingestion for all configured instruments."""
    try:
        # Initialise logger for Yahoo Finance component
        setup_logger("yahoo_finance")
        logger.info("Starting Yahoo Finance data ingestion")

        config_data = get_settings()
        config = YahooFinanceConfig(
            bucket_name=config_data.gcp.bucket_name,
            tickers=config_data.yahoo_finance.tickers,
        )

        processor = YahooFinanceProcessor(config)
        locations = await processor.process()

        logger.info(f"Successfully stored data at {len(locations)} locations")

    except Exception:
        logger.exception("Fatal error in Yahoo Finance ingestion")
        raise
