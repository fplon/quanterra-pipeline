"""Unit tests for the core manifest implementation."""

from typing import Any, Dict

import pytest
from pydantic import ValidationError

from src.ingest.core.manifest import PipelineManifest, ProcessorManifest, ProcessorType


class TestProcessorType:
    """Test suite for ProcessorType enum."""

    def test_processor_type_values(self) -> None:
        """Test that ProcessorType enum has correct values."""
        # OANDA
        assert ProcessorType.OANDA_INSTRUMENTS.value == "oanda_instruments"
        assert ProcessorType.OANDA_CANDLES.value == "oanda_candles"
        # EODHD
        assert ProcessorType.EODHD_EXCHANGE.value == "eodhd_exchange"
        assert ProcessorType.EODHD_EXCHANGE_SYMBOL.value == "eodhd_exchange_symbol"
        assert ProcessorType.EODHD_INSTRUMENT.value == "eodhd_instrument"
        assert ProcessorType.EODHD_MACRO.value == "eodhd_macro"
        assert ProcessorType.EODHD_ECONOMIC_EVENT.value == "eodhd_economic_event"
        # Yahoo Finance
        assert ProcessorType.YF_MARKET.value == "yf_market"

    def test_processor_type_from_string(self) -> None:
        """Test that ProcessorType can be created from string values."""
        assert ProcessorType("oanda_instruments") == ProcessorType.OANDA_INSTRUMENTS
        assert ProcessorType("eodhd_exchange") == ProcessorType.EODHD_EXCHANGE
        assert ProcessorType("yf_market") == ProcessorType.YF_MARKET

    def test_processor_type_invalid_value(self) -> None:
        """Test that ProcessorType raises error for invalid values."""
        with pytest.raises(ValueError) as exc_info:
            ProcessorType("invalid_type")
        assert "'invalid_type' is not a valid ProcessorType" in str(exc_info.value)


class TestProcessorManifest:
    """Test suite for ProcessorManifest class."""

    def test_processor_manifest_minimal_initialization(self) -> None:
        """Test that ProcessorManifest is correctly initialised with minimal fields."""
        manifest = ProcessorManifest(type=ProcessorType.OANDA_INSTRUMENTS)

        assert manifest.type == ProcessorType.OANDA_INSTRUMENTS
        assert manifest.config == {}
        assert manifest.depends_on is None

    def test_processor_manifest_full_initialization(self) -> None:
        """Test that ProcessorManifest is correctly initialised with all fields."""
        config: Dict[str, Any] = {"key": "value", "number": 42}
        depends_on = ["other_processor"]

        manifest = ProcessorManifest(
            type=ProcessorType.EODHD_EXCHANGE,
            config=config,
            depends_on=depends_on,
        )

        assert manifest.type == ProcessorType.EODHD_EXCHANGE
        assert manifest.config == config
        assert manifest.depends_on == depends_on

    def test_processor_manifest_missing_required_fields(self) -> None:
        """Test that ProcessorManifest raises error when missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessorManifest()  # type: ignore

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("type",)
        assert errors[0]["type"] == "missing"

    def test_processor_manifest_invalid_types(self) -> None:
        """Test that ProcessorManifest validates field types."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessorManifest(
                type="invalid_type",  # type: ignore
                config="not a dict",  # type: ignore
                depends_on="not a list",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 3

    def test_processor_manifest_config_mutation(self) -> None:
        """Test that config can be mutated after initialisation."""
        manifest = ProcessorManifest(type=ProcessorType.YF_MARKET)
        assert manifest.config == {}

        manifest.config["key"] = "value"
        assert manifest.config == {"key": "value"}

        manifest.config.update({"another_key": 42})
        assert manifest.config == {"key": "value", "another_key": 42}


class TestPipelineManifest:
    """Test suite for PipelineManifest class."""

    def test_pipeline_manifest_minimal_initialization(self) -> None:
        """Test that PipelineManifest is correctly initialised with minimal fields."""
        processors = [ProcessorManifest(type=ProcessorType.OANDA_INSTRUMENTS)]
        settings: Dict[str, Any] = {}

        manifest = PipelineManifest(
            name="test_pipeline",
            processors=processors,
            settings=settings,
        )

        assert manifest.name == "test_pipeline"
        assert manifest.processors == processors
        assert manifest.settings == settings

    def test_pipeline_manifest_full_initialization(self) -> None:
        """Test that PipelineManifest is correctly initialised with all fields."""
        processors = [
            ProcessorManifest(
                type=ProcessorType.EODHD_EXCHANGE,
                config={"key": "value"},
                depends_on=["other_processor"],
            ),
            ProcessorManifest(
                type=ProcessorType.EODHD_INSTRUMENT,
                config={"another_key": 42},
            ),
        ]
        settings: Dict[str, Any] = {"setting1": "value1", "setting2": 42}

        manifest = PipelineManifest(
            name="test_pipeline",
            processors=processors,
            settings=settings,
        )

        assert manifest.name == "test_pipeline"
        assert manifest.processors == processors
        assert manifest.settings == settings

    def test_pipeline_manifest_missing_required_fields(self) -> None:
        """Test that PipelineManifest raises error when missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineManifest()  # type: ignore

        errors = exc_info.value.errors()
        assert len(errors) == 3
        required_fields = {"name", "processors", "settings"}
        error_fields = {str(error["loc"][0]) for error in errors}
        assert error_fields == required_fields

    def test_pipeline_manifest_invalid_types(self) -> None:
        """Test that PipelineManifest validates field types."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineManifest(
                name=42,  # type: ignore
                processors="not a list",  # type: ignore
                settings="not a dict",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 3

    def test_pipeline_manifest_settings_mutation(self) -> None:
        """Test that settings can be mutated after initialisation."""
        manifest = PipelineManifest(
            name="test_pipeline",
            processors=[ProcessorManifest(type=ProcessorType.YF_MARKET)],
            settings={},
        )
        assert manifest.settings == {}

        manifest.settings["key"] = "value"
        assert manifest.settings == {"key": "value"}

        manifest.settings.update({"another_key": 42})
        assert manifest.settings == {"key": "value", "another_key": 42}
