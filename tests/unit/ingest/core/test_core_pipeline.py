"""Unit tests for the core pipeline implementation."""

from typing import Any, List, Optional, cast
from unittest.mock import AsyncMock, patch

import pytest

from src.common.models import StorageLocation
from src.ingest.core.context import PipelineContext, ProcessorMetrics
from src.ingest.core.pipeline import Pipeline
from src.ingest.core.processor import BaseProcessor


class MockProcessor(BaseProcessor):
    """Mock processor for testing."""

    def __init__(self, name: str, return_value: Any = None, should_fail: bool = False) -> None:
        """Initialise mock processor."""
        self._name = name
        self._return_value = return_value if return_value is not None else [1, 2, 3]
        self._should_fail = should_fail
        self._process_mock = AsyncMock()
        if should_fail:
            self._process_mock.side_effect = ValueError("Test error")
        else:
            self._process_mock.return_value = self._return_value

    @property
    def name(self) -> str:
        """Get processor name."""
        return self._name

    async def process(self, context: PipelineContext) -> Optional[List[StorageLocation]]:
        """Process data with mocked behaviour."""
        result = await self._process_mock(context)
        if result is None:
            return None
        return cast(List[StorageLocation], result)

    @property
    def was_called(self) -> bool:
        """Check if process was called."""
        return self._process_mock.called


@pytest.fixture
def mock_processor() -> MockProcessor:
    """Create a mock processor for testing."""
    return MockProcessor(name="test_processor")


@pytest.fixture
def mock_failing_processor() -> MockProcessor:
    """Create a mock processor that raises an exception."""
    return MockProcessor(name="failing_processor", should_fail=True)


class TestPipeline:
    """Test suite for Pipeline class."""

    def test_pipeline_initialisation(self, mock_processor: MockProcessor) -> None:
        """Test that pipeline is correctly initialised with name and processors."""
        name = "test_pipeline"
        processors: List[BaseProcessor] = [mock_processor]
        pipeline = Pipeline(name=name, processors=processors)

        assert pipeline.name == name
        assert pipeline.processors == processors
        assert len(pipeline.processors) == 1
        assert pipeline.processors[0].name == "test_processor"

    @pytest.mark.asyncio
    async def test_successful_pipeline_execution(self, mock_processor: MockProcessor) -> None:
        """Test successful execution of pipeline with single processor."""
        pipeline = Pipeline(name="test_pipeline", processors=[mock_processor])

        with patch("uuid.uuid4", return_value="test-uuid"):
            context = await pipeline.execute()

        assert isinstance(context, PipelineContext)
        assert context.pipeline_id == "test_pipeline-test-uuid"
        assert context.end_time is not None

        # Check processor metrics
        assert mock_processor.name in context.processor_metrics
        metrics = context.processor_metrics[mock_processor.name]
        assert isinstance(metrics, ProcessorMetrics)
        assert metrics.success is True
        assert metrics.records_processed == 3
        assert metrics.start_time is not None
        assert metrics.end_time is not None
        assert metrics.error_message is None

    @pytest.mark.asyncio
    async def test_multiple_processor_execution(self, mock_processor: MockProcessor) -> None:
        """Test successful execution of pipeline with multiple processors."""
        processors: List[BaseProcessor] = [
            mock_processor,
            MockProcessor(name="second_processor", return_value=[1, 2]),
            MockProcessor(name="third_processor", return_value=[1, 2]),
        ]

        pipeline = Pipeline(name="test_pipeline", processors=processors)
        context = await pipeline.execute()

        assert len(context.processor_metrics) == 3
        for processor in processors:
            assert processor.name in context.processor_metrics
            metrics = context.processor_metrics[processor.name]
            assert metrics.success is True
            assert metrics.error_message is None
            assert metrics.start_time is not None
            assert metrics.end_time is not None

    # FIXME - do I want to raise an error on failure or just say it failed?
    @pytest.mark.asyncio
    async def test_pipeline_processor_failure(
        self, mock_processor: MockProcessor, mock_failing_processor: MockProcessor
    ) -> None:
        """Test pipeline execution when a processor fails."""
        pipeline = Pipeline(
            name="test_pipeline",
            processors=[mock_processor, mock_failing_processor],
        )

        # with pytest.raises(ValueError) as exc_info:
        #     await pipeline.execute()
        context = await pipeline.execute()

        # assert str(exc_info.value) == "Test error"
        assert mock_processor.was_called
        assert mock_failing_processor.was_called
        assert context.processor_metrics["test_processor"].success
        assert not context.processor_metrics["failing_processor"].success

    @pytest.mark.asyncio
    async def test_pipeline_metrics_on_failure(
        self, mock_processor: MockProcessor, mock_failing_processor: MockProcessor
    ) -> None:
        """Test that metrics are properly recorded even when a processor fails."""
        pipeline = Pipeline(
            name="test_pipeline",
            processors=[mock_processor, mock_failing_processor],
        )

        context = None
        try:
            context = await pipeline.execute()
        except ValueError:
            pass

        assert context is not None
        # First processor should have succeeded
        metrics = context.processor_metrics[mock_processor.name]
        assert metrics.success is True
        assert metrics.records_processed == 3
        assert metrics.error_message is None

        # Second processor should have failed
        metrics = context.processor_metrics[mock_failing_processor.name]
        assert metrics.success is False
        assert metrics.records_processed == 0
        assert metrics.error_message == "Test error"

    @pytest.mark.asyncio
    async def test_empty_processor_list(self) -> None:
        """Test pipeline execution with no processors."""
        pipeline = Pipeline(name="test_pipeline", processors=[])
        context = await pipeline.execute()

        assert isinstance(context, PipelineContext)
        assert len(context.processor_metrics) == 0
        assert context.end_time is not None

    @pytest.mark.asyncio
    async def test_processor_with_no_results(self) -> None:
        """Test pipeline execution with a processor that returns no results."""
        processor = MockProcessor(name="no_results_processor", return_value=None)

        pipeline = Pipeline(name="test_pipeline", processors=[processor])
        context = await pipeline.execute()

        metrics = context.processor_metrics[processor.name]
        assert metrics.success is True
        # assert metrics.records_processed == 3
        assert metrics.error_message is None

    @pytest.mark.asyncio
    async def test_context_timing(self, mock_processor: MockProcessor) -> None:
        """Test that pipeline execution properly records timing information."""
        pipeline = Pipeline(name="test_pipeline", processors=[mock_processor])

        context = await pipeline.execute()

        assert context.start_time is not None
        assert context.end_time is not None
        assert context.start_time < context.end_time

        processor_metrics = context.processor_metrics[mock_processor.name]
        assert processor_metrics.start_time is not None
        assert processor_metrics.end_time is not None
        assert processor_metrics.start_time >= context.start_time
        assert processor_metrics.end_time <= context.end_time
