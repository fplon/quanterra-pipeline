from datetime import datetime
from pathlib import Path

import click
from prefect import flow
from prefect.deployments import run_deployment
from pydantic import BaseModel

from src.cli.tool_update import CLIToolUpdater
from src.clients.google_cloud_storage_client import GCPStorageClient
from src.models.config.pipeline_settings import Environment


@click.group()
@click.version_option()
def cli():
    """CLI for uploading transaction files to Quanterra datalake"""
    updater = CLIToolUpdater(bucket_name="datalake-dev-bronze")  # TODO update if changing
    updater.check_for_updates()


def _upload_to_gcs(source_path: Path, env: Environment) -> str:
    """Upload local file to GCS and return blob path"""
    if not source_path.exists():
        raise FileNotFoundError(f"File not found: {source_path}")

    client = GCPStorageClient()
    gcs_path = f"temp_uploads/{datetime.now().isoformat()}/{source_path.name}"

    client.store_csv_file(
        source_path=str(source_path),
        # TODO update with staging bucket once its set up
        bucket_name="datalake-dev-bronze" if env == Environment.DEV else "datalake-prod-bronze",
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
    """Trigger a Prefect flow run with the given parameters."""

    deployment_run = run_deployment(
        name=f"{flow}/{deployment}",
        parameters=parameters.model_dump(exclude_unset=True),
    )
    return deployment_run.id


@cli.command()
@click.option("--env", type=click.Choice(["dev", "prod"]), default="dev")
@click.option("--portfolio-name", required=True, help="Name of the portfolio")
@click.option("--transactions-path", required=True, type=Path, help="Path to transactions CSV")
def interactive_investor(env: str, portfolio_name: str, transactions_path: Path):
    """Upload Interactive Investor transactions"""
    env_enum = Environment(env)
    gcs_path = _upload_to_gcs(transactions_path, env_enum)

    flow_id = trigger_prefect_flow(
        flow="interactive_investor_transactions",
        deployment=f"{env}-interative-investor-pipeline",
        parameters=PrefectFlowParams(
            transactions_source_path=gcs_path,
            portfolio_name=portfolio_name,
            env=env,
        ),
    )
    click.echo(f"Flow run created: {flow_id}")


@cli.command()
@click.option("--env", type=click.Choice(["dev", "prod"]), default="dev")
@click.option("--portfolio-name", required=True, help="Name of the portfolio")
@click.option("--transactions-path", required=True, type=Path, help="Path to transactions CSV")
@click.option("--positions-path", required=True, type=Path, help="Path to positions CSV")
@click.option(
    "--closed-positions-path", required=True, type=Path, help="Path to closed positions CSV"
)
def hargreaves_lansdown(
    env: str,
    portfolio_name: str,
    transactions_path: Path,
    positions_path: Path,
    closed_positions_path: Path,
):
    """Upload Hargreaves Lansdown transactions"""
    env_enum = Environment(env)

    paths = {
        "transactions_source_path": _upload_to_gcs(transactions_path, env_enum),
        "positions_source_path": _upload_to_gcs(positions_path, env_enum),
        "closed_positions_source_path": _upload_to_gcs(closed_positions_path, env_enum),
    }

    flow_id = trigger_prefect_flow(
        flow="hargreaves_lansdown_transactions",
        deployment=f"{env}-hargreaves-lansdown-pipeline",
        parameters=PrefectFlowParams(
            **paths,
            portfolio_name=portfolio_name,
            env=env,
        ),
    )

    click.echo(f"Flow run created: {flow_id}")
