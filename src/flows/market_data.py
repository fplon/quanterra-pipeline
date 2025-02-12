import asyncio

from prefect import flow

from .eodhd import eodhd_market_data_flow
from .oanda import oanda_market_data_flow
from .yahoo_finance import yahoo_finance_market_data_flow


@flow
async def run_market_data_ingestion(env: str = "dev") -> None:
    market_data_flows = [
        eodhd_market_data_flow(env),
        oanda_market_data_flow(env),
        yahoo_finance_market_data_flow(env),
    ]
    await asyncio.gather(*market_data_flows)
