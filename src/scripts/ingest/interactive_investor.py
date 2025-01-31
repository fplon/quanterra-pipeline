from loguru import logger

from src.common.config import load_yaml_config, resolve_env_vars
from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.core.manifest import PipelineManifest
from src.ingest.core.pipeline import Pipeline
from src.ingest.data_sources.interactive_investor.models import InteractiveInvestorConfig
from src.ingest.data_sources.interactive_investor.processors import InteractiveInvestorProcessor


async def run_interactive_investor_ingestion() -> None:
    """Run Interactive Investor data ingestion pipeline."""
    try:
        # Initialise logger
        setup_logger("interactive_investor")
        logger.info("Starting Interactive Investor data ingestion")

        # Load and parse manifest
        manifest_path = "src/ingest/config/manifests/interactive_investor.yml"
        raw_manifest = load_yaml_config(manifest_path)
        manifest = PipelineManifest.model_validate(resolve_env_vars(raw_manifest))

        # Create config
        settings = get_settings()
        config = InteractiveInvestorConfig(
            bucket_name=settings.gcp.bucket_name,
            source_path=manifest.settings["source_path"],
        )

        # Create processor
        processor = InteractiveInvestorProcessor(config=config)

        # Run pipeline
        pipeline = Pipeline(name=manifest.name, processors=[processor])
        context = await pipeline.execute()

        if not context.end_time:
            raise RuntimeError("Pipeline failed to complete")

        logger.success(
            f"Pipeline completed in {(context.end_time - context.start_time).total_seconds():.2f}s"
        )
    except Exception:
        logger.exception("Error running Interactive Investor ingestion pipeline")
        raise
