from loguru import logger

from src.common.config import load_yaml_config, resolve_env_vars
from src.common.logging.config import setup_logger
from src.ingest.config.settings import get_settings
from src.ingest.core.manifest import PipelineManifest
from src.ingest.core.pipeline import Pipeline
from src.ingest.data_sources.yahoo_finance.models import YahooFinanceConfig
from src.ingest.data_sources.yahoo_finance.processors import YahooFinanceProcessor


async def run_yahoo_finance_ingestion(manifest: PipelineManifest | None = None) -> None:
    """Run Yahoo Finance data ingestion pipeline."""
    try:
        # Initialise logger for Yahoo Finance component
        setup_logger("yahoo_finance")
        logger.info("Starting Yahoo Finance data ingestion")

        # Load and parse manifest
        if not manifest:
            manifest_path = "src/ingest/config/manifests/yahoo_finance.yml"
            raw_manifest = load_yaml_config(manifest_path)
            manifest = PipelineManifest.model_validate(resolve_env_vars(raw_manifest))

        # Create processor
        config_data = get_settings()
        config = YahooFinanceConfig(
            bucket_name=config_data.gcp.bucket_name,
            tickers=config_data.yahoo_finance.tickers,
        )
        processor = YahooFinanceProcessor(config)

        # Create and execute pipeline
        pipeline = Pipeline(name=manifest.name, processors=[processor])
        context = await pipeline.execute()

        if not context.end_time:
            raise RuntimeError("Pipeline failed to complete")

        logger.success(
            f"Pipeline completed in {(context.end_time - context.start_time).total_seconds():.2f}s"
        )

    except Exception:
        logger.exception("Fatal error in Yahoo Finance ingestion")
        raise
