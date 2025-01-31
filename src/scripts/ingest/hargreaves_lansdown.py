from loguru import logger

from src.common.config import load_yaml_config, resolve_env_vars
from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.core.manifest import PipelineManifest
from src.ingest.core.pipeline import Pipeline
from src.ingest.data_sources.hargreaves_lansdown.models import HargreavesLansdownConfig
from src.ingest.data_sources.hargreaves_lansdown.processors import HargreavesLansdownProcessor


async def run_hargreaves_lansdown_ingestion() -> None:
    """Run Hargreaves Lansdown data ingestion pipeline."""
    try:
        # Initialise logger
        setup_logger("hargreaves_lansdown")
        logger.info("Starting Hargreaves Lansdown data ingestion")

        # Load and parse manifest
        manifest_path = "src/ingest/config/manifests/hargreaves_lansdown.yml"
        raw_manifest = load_yaml_config(manifest_path)
        manifest = PipelineManifest.model_validate(resolve_env_vars(raw_manifest))

        # Create config
        settings = get_settings()
        config = HargreavesLansdownConfig(
            bucket_name=settings.gcp.bucket_name,
            transactions_source_path=manifest.settings["transactions_source_path"],
            positions_source_path=manifest.settings["positions_source_path"],
            closed_positions_source_path=manifest.settings["closed_positions_source_path"],
        )

        # Create processor
        processor = HargreavesLansdownProcessor(config=config)

        # Run pipeline
        pipeline = Pipeline(name=manifest.name, processors=[processor])
        context = await pipeline.execute()

        if not context.end_time:
            raise RuntimeError("Pipeline failed to complete")

        logger.success(
            f"Pipeline completed in {(context.end_time - context.start_time).total_seconds():.2f}s"
        )
    except Exception:
        logger.exception("Error running Hargreaves Lansdown ingestion pipeline")
        raise
