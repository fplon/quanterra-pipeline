from loguru import logger

from src.common.config import load_yaml_config, resolve_env_vars
from src.common.logging.config import setup_logger
from src.ingest.core.manifest import PipelineManifest, ProcessorType
from src.ingest.core.pipeline import Pipeline
from src.ingest.core.processor import BaseProcessor
from src.ingest.data_sources.eodhd.factory import EODHDProcessorFactory
from src.ingest.data_sources.eodhd.models import EODHDConfig


async def run_eodhd_ingestion(manifest: PipelineManifest | None = None) -> None:
    """Run EODHD data ingestion pipeline."""
    try:
        # Initialise logger for EODHD component
        setup_logger("eodhd")
        logger.info("Starting EODHD data ingestion")

        # Load and parse manifest
        if not manifest:
            manifest_path = "src/ingest/config/manifests/eodhd.yml"
            raw_manifest = load_yaml_config(manifest_path)
            manifest = PipelineManifest.model_validate(resolve_env_vars(raw_manifest))

        # Create processor factory
        factory = EODHDProcessorFactory()

        # Create processors from manifest
        processors: list[BaseProcessor] = []
        for proc_manifest in manifest.processors:
            processor = factory.create_processor(
                processor_type=ProcessorType(proc_manifest.type),
                config=EODHDConfig(**manifest.settings),
            )
            processors.append(processor)

        # Create and execute pipeline
        pipeline = Pipeline(name=manifest.name, processors=processors)
        context = await pipeline.execute()

        if not context.end_time:
            raise RuntimeError("Pipeline failed to complete")

        logger.success(
            f"Pipeline completed in {(context.end_time - context.start_time).total_seconds():.2f}s"
        )

    except Exception:
        logger.exception("Fatal error in EODHD ingestion")
        raise
