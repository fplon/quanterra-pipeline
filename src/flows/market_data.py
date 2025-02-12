from prefect import flow

from .eodhd import eodhd_market_data_flow
from .oanda import oanda_market_data_flow
from .yahoo_finance import yahoo_finance_market_data_flow


@flow
async def run_market_data_ingestion(env: str = "dev") -> None:
    """
    Run the market data ingestion flows sequentially.
    """

    await yahoo_finance_market_data_flow(env)
    await eodhd_market_data_flow(env)
    await oanda_market_data_flow(env)
