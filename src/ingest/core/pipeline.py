import uuid
from datetime import datetime
from typing import List

from loguru import logger

from src.ingest.core.context import PipelineContext, ProcessorMetrics
from src.ingest.core.processor import BaseProcessor


class Pipeline:
    def __init__(
        self,
        name: str,
        processors: List[BaseProcessor],
    ):
        self.name = name
        self.processors = processors

    async def execute(self) -> PipelineContext:
        context = PipelineContext(pipeline_id=f"{self.name}-{uuid.uuid4()}")

        try:
            for processor in self.processors:
                context.processor_metrics[processor.name] = ProcessorMetrics(
                    start_time=datetime.now()
                )

                try:
                    result = await processor.process(context)
                    metrics = context.processor_metrics[processor.name]
                    metrics.end_time = datetime.now()
                    metrics.success = True
                    metrics.records_processed = len(result) if result else 0

                except Exception as e:
                    logger.exception(f"Processor {processor.name} failed")
                    metrics = context.processor_metrics[processor.name]
                    metrics.end_time = datetime.now()
                    metrics.success = False
                    metrics.error_message = str(e)
                    raise

        finally:
            context.end_time = datetime.now()
            return context
