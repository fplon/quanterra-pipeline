from enum import Enum

from .models import EODHDConfig
from .services import (
    EconomicEventDataService,
    EODIngestionService,
    ExchangeDataService,
    ExchangeSymbolService,
    InstrumentDataService,
    MacroDataService,
)


class ServiceType(Enum):
    EXCHANGE = "exchange"
    EXCHANGE_SYMBOL = "exchange_symbol"
    INSTRUMENT = "instrument"
    MACRO = "macro-indicator"
    ECONOMIC_EVENT = "economic_event"


class EODHDServiceFactory:
    """Factory for creating EODHD ingestion services."""

    @staticmethod
    def create_service(
        service_type: ServiceType,
        config: EODHDConfig,
    ) -> EODIngestionService:
        if service_type == ServiceType.EXCHANGE:
            return ExchangeDataService(config)
        elif service_type == ServiceType.EXCHANGE_SYMBOL:
            return ExchangeSymbolService(config)
        elif service_type == ServiceType.INSTRUMENT:
            return InstrumentDataService(config)
        elif service_type == ServiceType.MACRO:
            return MacroDataService(config)
        elif service_type == ServiceType.ECONOMIC_EVENT:
            return EconomicEventDataService(config)
