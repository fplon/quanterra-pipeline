"""Unit tests for the core context implementation."""

from datetime import datetime, timedelta
from typing import Any, Dict

import pytest
from pydantic import ValidationError

from src.ingest.core.context import PipelineContext, ProcessorMetrics


class TestProcessorMetrics:
    """Test suite for ProcessorMetrics class."""

    def test_processor_metrics_initialisation(self) -> None:
        """Test that ProcessorMetrics is correctly initialised with required fields."""
        start_time = datetime.now()
        metrics = ProcessorMetrics(start_time=start_time)

        assert metrics.start_time == start_time
        assert metrics.end_time is None
        assert metrics.success is False
        assert metrics.error_message is None
        assert metrics.records_processed == 0

    def test_processor_metrics_full_initialisation(self) -> None:
        """Test that ProcessorMetrics is correctly initialised with all fields."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        metrics = ProcessorMetrics(
            start_time=start_time,
            end_time=end_time,
            success=True,
            error_message="test error",
            records_processed=42,
        )

        assert metrics.start_time == start_time
        assert metrics.end_time == end_time
        assert metrics.success is True
        assert metrics.error_message == "test error"
        assert metrics.records_processed == 42

    def test_processor_metrics_missing_required_fields(self) -> None:
        """Test that ProcessorMetrics raises error when missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessorMetrics()  # type: ignore

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("start_time",)
        assert errors[0]["type"] == "missing"

    def test_processor_metrics_invalid_types(self) -> None:
        """Test that ProcessorMetrics validates field types."""
        with pytest.raises(ValidationError) as exc_info:
            ProcessorMetrics(
                start_time=datetime.now(),
                end_time="invalid",  # type: ignore
                success="not a bool",  # type: ignore
                error_message=42,  # type: ignore
                records_processed="not an int",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 4


class TestPipelineContext:
    """Test suite for PipelineContext class."""

    def test_pipeline_context_initialisation(self) -> None:
        """Test that PipelineContext is correctly initialised with required fields."""
        context = PipelineContext(pipeline_id="test-pipeline")

        assert context.pipeline_id == "test-pipeline"
        assert isinstance(context.start_time, datetime)
        assert context.end_time is None
        assert context.shared_state == {}
        assert context.processor_metrics == {}

    def test_pipeline_context_full_initialisation(self) -> None:
        """Test that PipelineContext is correctly initialised with all fields."""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=1)
        shared_state: Dict[str, Any] = {"key": "value"}
        processor_metrics = {"test_processor": ProcessorMetrics(start_time=start_time)}

        context = PipelineContext(
            pipeline_id="test-pipeline",
            start_time=start_time,
            end_time=end_time,
            shared_state=shared_state,
            processor_metrics=processor_metrics,
        )

        assert context.pipeline_id == "test-pipeline"
        assert context.start_time == start_time
        assert context.end_time == end_time
        assert context.shared_state == shared_state
        assert context.processor_metrics == processor_metrics

    def test_pipeline_context_missing_required_fields(self) -> None:
        """Test that PipelineContext raises error when missing required fields."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineContext()  # type: ignore

        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert errors[0]["loc"] == ("pipeline_id",)
        assert errors[0]["type"] == "missing"

    def test_pipeline_context_invalid_types(self) -> None:
        """Test that PipelineContext validates field types."""
        with pytest.raises(ValidationError) as exc_info:
            PipelineContext(
                pipeline_id=42,  # type: ignore
                start_time="invalid",  # type: ignore
                end_time="invalid",  # type: ignore
                shared_state="not a dict",  # type: ignore
                processor_metrics="not a dict",  # type: ignore
            )

        errors = exc_info.value.errors()
        assert len(errors) == 5

    def test_pipeline_context_shared_state_mutation(self) -> None:
        """Test that shared state can be mutated after initialisation."""
        context = PipelineContext(pipeline_id="test-pipeline")
        assert context.shared_state == {}

        context.shared_state["key"] = "value"
        assert context.shared_state == {"key": "value"}

        context.shared_state.update({"another_key": 42})
        assert context.shared_state == {"key": "value", "another_key": 42}

    def test_pipeline_context_processor_metrics_mutation(self) -> None:
        """Test that processor metrics can be added after initialisation."""
        context = PipelineContext(pipeline_id="test-pipeline")
        assert context.processor_metrics == {}

        metrics = ProcessorMetrics(start_time=datetime.now())
        context.processor_metrics["test_processor"] = metrics
        assert context.processor_metrics == {"test_processor": metrics}

        another_metrics = ProcessorMetrics(start_time=datetime.now())
        context.processor_metrics["another_processor"] = another_metrics
        assert context.processor_metrics == {
            "test_processor": metrics,
            "another_processor": another_metrics,
        }

    def test_pipeline_context_default_factories(self) -> None:
        """Test that default factories create new instances for each context."""
        context1 = PipelineContext(pipeline_id="pipeline1")
        context2 = PipelineContext(pipeline_id="pipeline2")

        assert context1.shared_state is not context2.shared_state
        assert context1.processor_metrics is not context2.processor_metrics

        context1.shared_state["key"] = "value"
        assert "key" not in context2.shared_state

        metrics = ProcessorMetrics(start_time=datetime.now())
        context1.processor_metrics["test"] = metrics
        assert "test" not in context2.processor_metrics
