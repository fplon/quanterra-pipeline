from enum import Enum

from .models import EODHDConfig
from .services import (
    EODIngestionService,
    ExchangeDataService,
    ExchangeSymbolService,
    InstrumentDataService,
)


class ServiceType(Enum):
    EXCHANGE = "exchange"
    EXCHANGE_SYMBOL = "exchange_symbol"
    INSTRUMENT = "instrument"


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
        else:
            raise ValueError(f"Unknown service type: {service_type}")
