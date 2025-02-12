from datetime import datetime

from loguru import logger
from prefect import task

from src.common.gcp.client import GCPStorageClient
from src.common.models import StorageLocation
from src.ingest.data_sources.yahoo_finance.client import YahooFinanceClient
from src.ingest.data_sources.yahoo_finance.models import (
    MarketData,
    TickerData,
    YahooFinanceConfig,
)


@task(name="store_yahoo_finance_data", retries=3)
async def store_data(
    data: MarketData | TickerData,
    location: StorageLocation,
    config: YahooFinanceConfig,
) -> None:
    """Store Yahoo Finance data in Google Cloud Storage."""
    storage_client = GCPStorageClient(credentials=config.gcp_credentials)
    storage_client.store_json_data(
        data=data.to_json(),
        bucket_name=location.bucket,
        blob_path=location.path,
        compress=True,
    )


@task(name="fetch_yahoo_finance_tickers")
async def fetch_tickers(config: YahooFinanceConfig, client: YahooFinanceClient) -> None:
    """Fetch and store Yahoo Finance ticker data."""
    logger.info("Fetching Yahoo Finance ticker data")

    for ticker in config.tickers:
        logger.info(f"Fetching ticker data for {ticker}")
        try:
            raw_data = client.get_tickers_data([ticker])
            data = TickerData(
                data=raw_data,
                timestamp=datetime.now(),
                data_type="tickers",
                ticker=ticker,
            )
            location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())

            await store_data(data, location, config)
            logger.success(f"Stored ticker data for {ticker} at: {location}")

        except Exception:
            logger.exception(f"Error fetching ticker data for {ticker}")
            raise


@task(name="fetch_yahoo_finance_market_data")
async def fetch_market_data(
    config: YahooFinanceConfig,
    client: YahooFinanceClient,
    period: str = "max",
    interval: str = "1d",
) -> None:
    """Fetch and store Yahoo Finance market data."""
    logger.info("Fetching Yahoo Finance market data")

    for ticker in config.tickers:
        logger.info(f"Fetching market data for {ticker}")
        try:
            raw_data = client.get_market_data([ticker], period=period, interval=interval)
            data = MarketData(
                data=raw_data,
                timestamp=datetime.now(),
                data_type="market",
                ticker=ticker,
            )
            location = StorageLocation(bucket=config.bucket_name, path=data.get_storage_path())

            await store_data(data, location, config)
            logger.success(f"Stored market data for {ticker} at: {location}")

        except Exception:
            logger.exception(f"Error fetching market data for {ticker}")
            raise
