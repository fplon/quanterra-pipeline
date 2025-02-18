from typing import Literal

from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from pydantic import BaseModel, Field


class SimpleFlowSettings(BaseModel):
    source_bucket_name: str | None = None
    target_bucket_name: str


class EODHDFlowSettings(SimpleFlowSettings):
    exchanges: list[str]
    exchanges_bulk: list[str]
    instruments: list[str]
    macro_indicators: list[str]
    macro_countries: list[str]


class EODHDConfig(BaseModel):
    """Configuration for EODHD data ingestion."""

    api_key: str
    base_url: str
    bucket_name: str
    exchanges: list[str] = Field(default_factory=list)
    exchanges_bulk: list[str] = Field(default_factory=list)
    instruments: list[str] = Field(default_factory=list)
    macro_indicators: list[str] = Field(default_factory=list)
    macro_countries: list[str] = Field(default_factory=list)
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class OANDAFlowSettings(SimpleFlowSettings):
    instruments: list[str]
    granularity: Literal["D", "M1"]
    count: int


class OANDAConfig(BaseModel):
    """Configuration for OANDA data ingestion."""

    api_key: str
    base_url: str
    bucket_name: str
    account_id: str
    granularity: str
    count: int
    price: str = "MBA"
    instruments: list[str] | None = None
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class YahooFinanceFlowSettings(SimpleFlowSettings):
    tickers: list[str]


class YahooFinanceConfig(BaseModel):
    """Configuration for Yahoo Finance data ingestion."""

    bucket_name: str
    tickers: list[str]
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class HargreavesLansdownConfig(BaseModel):
    """Configuration for Hargreaves Lansdown data ingestion."""

    source_bucket_name: str
    target_bucket_name: str
    portfolio_name: str = "unassigned"
    transactions_source_path: str | None = None
    positions_source_path: str | None = None
    closed_positions_source_path: str | None = None
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    # TODO make this part of a base model
    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }


class InteractiveInvestorConfig(BaseModel):
    """Configuration for Interactive Investor data ingestion."""

    source_bucket_name: str
    target_bucket_name: str
    source_path: str
    portfolio_name: str
    gcp_credentials: Credentials | ServiceAccountCredentials | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "protected_namespaces": (),
    }
