from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class ProcessorMetrics(BaseModel):
    start_time: datetime
    end_time: datetime | None = None
    success: bool = False
    error_message: str | None = None
    records_processed: int = 0


class PipelineContext(BaseModel):
    pipeline_id: str
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: datetime | None = None
    shared_state: dict[str, Any] = Field(default_factory=dict)
    processor_metrics: dict[str, ProcessorMetrics] = Field(default_factory=dict)
