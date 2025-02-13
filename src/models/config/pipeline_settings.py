from enum import Enum
from typing import Generic, Type, TypeVar, cast

from pydantic import BaseModel

from src.models.config.processor_settings import (
    EODHDFlowSettings,
    OANDAFlowSettings,
    SimpleFlowSettings,
    YahooFinanceFlowSettings,
)


class StorageLocation(BaseModel):
    """Storage location details."""

    bucket: str
    path: str

    def __str__(self) -> str:
        return f"{self.bucket}/{self.path}"


class Environment(str, Enum):
    DEV = "dev"
    PROD = "prod"


T = TypeVar("T", bound=SimpleFlowSettings)


class BaseEnvironmentSettings(Generic[T]):
    _settings: dict[Environment, T]

    @classmethod
    def get_settings(cls: Type["BaseEnvironmentSettings[T]"], env: Environment) -> T:
        return cast(T, cls._settings[env])


class SimpleEnvironmentSettings(BaseEnvironmentSettings[SimpleFlowSettings]):
    _settings = {
        Environment.DEV: SimpleFlowSettings(
            bucket_name="datalake-dev-bronze",
        ),
        Environment.PROD: SimpleFlowSettings(
            bucket_name="datalake-prod-bronze",
        ),
    }


class EODHDEnvironmentSettings(BaseEnvironmentSettings[EODHDFlowSettings]):
    _settings = {
        Environment.DEV: EODHDFlowSettings(
            exchanges=["XETRA"],
            exchanges_bulk=["TO"],
            instruments=["AAPL.US", "GB00B9876293.EUFUND", "GB00BG0QPQ07.EUFUND", "IXIC.INDX"],
            macro_indicators=[
                "unemployment_total_percent",
                "inflation_consumer_prices_annual",
                "gdp_growth_annual",
            ],
            macro_countries=["GBR", "USA"],
            bucket_name="datalake-dev-bronze",
        ),
        Environment.PROD: EODHDFlowSettings(
            exchanges=[
                "INDX",
                "EUFUND",
            ],
            exchanges_bulk=[
                "US",
                "LSE",
                "XETRA",
                # "EUFUND",
                # "INDX",
            ],
            instruments=[],  # Get all instruments for the above exchanges
            macro_indicators=[
                "real_interest_rate",
                "population_total",
                "population_growth_annual",
                "inflation_consumer_prices_annual",
                "consumer_price_index",
                "gdp_current_usd",
                "gdp_per_capita_usd",
                "gdp_growth_annual",
                "debt_percent_gdp",
                "net_trades_goods_services",
                "inflation_gdp_deflator_annual",
                "agriculture_value_added_percent_gdp",
                "industry_value_added_percent_gdp",
                "services_value_added_percent_gdp",
                "exports_of_goods_services_percent_gdp",
                "imports_of_goods_services_percent_gdp",
                "gross_capital_formation_percent_gdp",
                "net_migration",
                "gni_usd",
                "gni_per_capita_usd",
                "gni_ppp_usd",
                "gni_per_capita_ppp_usd",
                "income_share_lowest_twenty",
                "life_expectancy",
                "fertility_rate",
                "prevalence_hiv_total",
                "co2_emissions_tons_per_capita",
                "surface_area_km",
                "poverty_poverty_lines_percent_population",
                "revenue_excluding_grants_percent_gdp",
                "cash_surplus_deficit_percent_gdp",
                "startup_procedures_register",
                "market_cap_domestic_companies_percent_gdp",
                "mobile_subscriptions_per_hundred",
                "internet_users_per_hundred",
                "high_technology_exports_percent_total",
                "merchandise_trade_percent_gdp",
                "total_debt_service_percent_gni",
                "unemployment_total_percent",
            ],
            macro_countries=[
                # G20 Nations
                "USA",  # United States
                "GBR",  # United Kingdom
                "DEU",  # Germany
                "FRA",  # France
                "ITA",  # Italy
                "CAN",  # Canada
                "JPN",  # Japan
                "AUS",  # Australia
                "CHN",  # China
                "IND",  # India
                "BRA",  # Brazil
                "MEX",  # Mexico
                "RUS",  # Russia
                "ZAF",  # South Africa
                "TUR",  # Turkey
                "KOR",  # South Korea
                "IDN",  # Indonesia
                "SAU",  # Saudi Arabia
                "ARG",  # Argentina
                # Additional Significant Economies
                "CHE",  # Switzerland
                "SGP",  # Singapore
                "NLD",  # Netherlands
                "HKG",  # Hong Kong
                "SWE",  # Sweden
                "NOR",  # Norway
                "IRL",  # Ireland
                "DNK",  # Denmark
                "FIN",  # Finland
                "NZL",  # New Zealand
                "ISR",  # Israel
                "ARE",  # United Arab Emirates
                "TWN",  # Taiwan
                "ESP",  # Spain
                "BEL",  # Belgium
                "AUT",  # Austria
            ],
            bucket_name="datalake-prod-bronze",
        ),
    }


class OANDAEnvironmentSettings(BaseEnvironmentSettings[OANDAFlowSettings]):
    _settings = {
        Environment.DEV: OANDAFlowSettings(
            instruments=["CAD_JPY", "EUR_USD", "GBP_USD", "USD_JPY"],
            granularity="D",
            count=50,
            bucket_name="datalake-dev-bronze",
        ),
        Environment.PROD: OANDAFlowSettings(
            instruments=[],  # Get all
            granularity="M1",
            count=60 * 24 * 3,
            bucket_name="datalake-prod-bronze",
        ),
    }


class YahooFinanceEnvironmentSettings(BaseEnvironmentSettings[YahooFinanceFlowSettings]):
    _settings = {
        Environment.DEV: YahooFinanceFlowSettings(
            tickers=[
                "0P0000XYZ1.L",
                "0P000102MS.L",
                "0P0000KSPA.L",
            ],
            bucket_name="datalake-dev-bronze",
        ),
        Environment.PROD: YahooFinanceFlowSettings(
            tickers=[
                "0P0000XYZ1.L",
                "0P000102MS.L",
                "0P0000KSPA.L",
                "0P0000X8NI.L",
                "0P00016MMX.L",
                "0P0000YZ0D.L",
                "0P0001C5T6.L",
                "0P0000WGST.L",
                "0P00000DIQ.L",
                "0P0000X9F5.L",
                "0P0000SAVS.L",
                "0P0000KSP8.L",
                "0P000023NA.L",
                "0P00009NF8.L",
                "0P0000WGSS.L",
                "0P00011WUD.L",
                "0P0000WGT4.L",
                "0P0000WGT5.L",
                "0P0000WGSW.L",
                "0P0000NQ2K.L",
                "0P0000WGSX.L",
                "0P0000YZ0C.L",
                "0P000023MW.L",
                "0P0000KM1Y.L",
                "0P0000JWBO.L",
                "0P000023CB.L",
                "0P00012D58.L",
                "0P0000YX0L.L",
                "0P0001C5T4.L",
                "0P00012D5A.L",
                "0P0000X9FA.L",
                "0P000023U7.L",
                "0P00001BWX.L",
                "0P00012D57.L",
                "0P000148T6.L",
                "0P0000XRSP.L",
                "0P0000X0B2.L",
                "0P0000XMYV.L",
                "0P00013MDA.L",
                "0P0000KM25.L",
                "0P0001CGI6.L",
                "0P000148T4.L",
                "0P000127UC.L",
                "0P0001FT5V.L",
                "0P000102MR.L",
                "0P000127U8.L",
                "0P00012KV4.L",
                "0P000127UA.L",
                "0P000179AN.L",
                "0P000102MP.L",
                "0P00012D56.L",
                "0P000127UB.L",
                "0P000148T3.L",
                "0P00016MMV.L",
                "0P000169US.L",
                "0P00000WPX.L",
                "0P0001H3KH.L",
                "VUAG.L",
                "0P0000KM22.L",
                "0P000102MM.L",
                "0P0001EI7Y.L",
            ],
            bucket_name="datalake-prod-bronze",
        ),
    }
