from pathlib import Path

import pytest
from click.testing import CliRunner

from src.cli.upload_transactions import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a Click CLI test runner."""
    return CliRunner()


# @pytest.mark.asyncio
def test_interactive_investor_upload(runner: CliRunner) -> None:
    """Test uploading Interactive Investor transactions through CLI."""
    # Arrange
    transactions_path = Path(".notes/ii_transactions_sample.csv")
    portfolio_name = "test_ii"

    # Act
    result = runner.invoke(
        cli,
        [
            "interactive-investor",
            "--env",
            "dev",
            "--portfolio-name",
            portfolio_name,
            "--transactions-path",
            str(transactions_path),
        ],
    )

    # Assert
    assert result.exit_code == 0
    assert "Flow run created:" in result.output


@pytest.mark.asyncio
async def test_hargreaves_lansdown_upload(runner: CliRunner) -> None:
    """Test uploading Hargreaves Lansdown transactions through CLI."""
    # Arrange
    transactions_path = Path(".notes/hl_transactions_sample.csv")
    positions_path = Path(".notes/hl_positions_sample.csv")
    closed_positions_path = Path(".notes/hl_closed_positions_sample.csv")
    portfolio_name = "test_hl"

    # Act
    result = runner.invoke(
        cli,
        [
            "hargreaves-lansdown",
            "--env",
            "dev",
            "--portfolio-name",
            portfolio_name,
            "--transactions-path",
            str(transactions_path),
            "--positions-path",
            str(positions_path),
            "--closed-positions-path",
            str(closed_positions_path),
        ],
    )

    # Assert
    assert result.exit_code == 0
    assert "Flow run created:" in result.output
