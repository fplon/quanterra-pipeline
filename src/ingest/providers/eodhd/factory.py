from enum import Enum
from typing import List

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
        data_types: List[str] | None = None,
    ) -> EODIngestionService:
        if service_type == ServiceType.EXCHANGE:
            return ExchangeDataService(config)
        elif service_type == ServiceType.EXCHANGE_SYMBOL:
            return ExchangeSymbolService(config)
        elif service_type == ServiceType.INSTRUMENT:
            if not data_types:
                raise ValueError("Data types required for instrument service")
            return InstrumentDataService(config, data_types)
        else:
            raise ValueError(f"Unknown service type: {service_type}")
