from datetime import datetime
from pathlib import Path

import click
from prefect import flow, get_run_logger
from prefect.deployments import run_deployment
from pydantic import BaseModel

from src.cli.tool_update import CLIToolUpdater
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import Environment


def log_cli_message(level: str, message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    click.echo(f"{timestamp} | {level:8} | {message}")


@click.group()
@click.version_option()
@click.option("--env", type=click.Choice(["dev", "prod"]), default="prod")
def cli(env: str = "prod"):
    """CLI for uploading transaction files to Quanterra datalake"""
    updater = CLIToolUpdater(bucket_name=f"datalake-{env}-cli-tool-config")
    updater.check_for_updates()


def _upload_to_gcs(source_path: Path, env: Environment) -> str:
    """Upload local file to GCS and return blob path"""
    if not source_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    client = GCPStorageClient()
    gcs_path = f"transactions_cli_uploads/{datetime.now().isoformat()}/{source_path.name}"

    client.store_csv_file(
        source_path=str(source_path),
        bucket_name="datalake-dev-landing" if env == Environment.DEV else "datalake-prod-landing",
        blob_path=gcs_path,
        compress=False,
    )
    return gcs_path


class PrefectFlowParams(BaseModel):
    transactions_source_path: str
    positions_source_path: str | None = None
    closed_positions_source_path: str | None = None
    portfolio_name: str
    env: str


@flow
def trigger_prefect_flow(flow: str, deployment: str, parameters: PrefectFlowParams) -> str:
    """Trigger a Prefect flow run with the given parameters and monitor its execution."""
    logger = get_run_logger()
    logger.info(f"Triggering deployment flow: {flow}/{deployment}")

    deployment_run = run_deployment(
        name=f"{flow}/{deployment}",
        parameters=parameters.model_dump(exclude_unset=True),
        poll_interval=5,  # Check status every 5 seconds
    )

    if deployment_run.state and deployment_run.state.is_failed():
        raise click.ClickException(
            f"Flow run failed with message: {deployment_run.state.message}\n"
            f"For more details, visit the flow run in Prefect Cloud."
        )

    logger.info(f"Flow run completed successfully: {deployment_run.id}")
    return str(deployment_run.id)


@cli.command()
@click.pass_context
@click.option("--portfolio-name", required=True, help="Name of the portfolio")
@click.option("--transactions-path", required=True, type=Path, help="Path to transactions CSV")
def interactive_investor(ctx, portfolio_name: str, transactions_path: Path):
    """Upload Interactive Investor transactions"""
    env = ctx.parent.params["env"]
    env_enum = Environment(env)

    log_cli_message("INFO", "Beginning Interactive Investor file upload")
    log_cli_message("INFO", f"Uploading file to GCS: {transactions_path}")
    gcs_path = _upload_to_gcs(transactions_path, env_enum)
    log_cli_message("INFO", f"File uploaded successfully to: {gcs_path}")

    flow_id = trigger_prefect_flow(
        flow="interactive_investor_transactions",
        deployment=f"{env}-interative-investor-pipeline",
        parameters=PrefectFlowParams(
            transactions_source_path=gcs_path,
            portfolio_name=portfolio_name,
            env=env,
        ),
    )
    log_cli_message("INFO", f"Flow run created: {flow_id}")


@cli.command()
@click.pass_context
@click.option("--portfolio-name", required=True, help="Name of the portfolio")
@click.option("--transactions-path", required=True, type=Path, help="Path to transactions CSV")
@click.option("--positions-path", required=True, type=Path, help="Path to positions CSV")
@click.option(
    "--closed-positions-path", required=True, type=Path, help="Path to closed positions CSV"
)
def hargreaves_lansdown(
    ctx,
    portfolio_name: str,
    transactions_path: Path,
    positions_path: Path,
    closed_positions_path: Path,
):
    """Upload Hargreaves Lansdown transactions"""
    env = ctx.parent.params["env"]
    env_enum = Environment(env)

    log_cli_message("INFO", "Beginning Hargreaves Lansdown file upload")
    log_cli_message("INFO", "Uploading files to GCS")

    paths = {
        "transactions_source_path": _upload_to_gcs(transactions_path, env_enum),
        "positions_source_path": _upload_to_gcs(positions_path, env_enum),
        "closed_positions_source_path": _upload_to_gcs(closed_positions_path, env_enum),
    }
    log_cli_message("INFO", "All files uploaded successfully")

    flow_id = trigger_prefect_flow(
        flow="hargreaves_lansdown_transactions",
        deployment=f"{env}-hargreaves-lansdown-pipeline",
        parameters=PrefectFlowParams(
            **paths,
            portfolio_name=portfolio_name,
            env=env,
        ),
    )
    log_cli_message("INFO", f"Flow run created: {flow_id}")
