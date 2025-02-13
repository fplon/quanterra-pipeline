from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class HargreavesLansdownBase(BaseModel):
    """Base model for Hargreaves Lansdown data."""

    data: list[list[str]]
    portfolio_name: str = "unassigned"
    timestamp: datetime = Field(default_factory=datetime.now)

    def get_storage_path(self) -> str:
        """Get the storage path for the data."""
        date_str = self.timestamp.strftime("%Y%m%d")
        return (
            f"transactions/hargreaves_lansdown/{self.portfolio_name}/{date_str}/transactions.csv.gz"
        )


class HargreavesLansdownTransaction(HargreavesLansdownBase):
    """Model for Hargreaves Lansdown transaction data."""

    @model_validator(mode="after")
    def validate_data_structure(self) -> "HargreavesLansdownTransaction":
        """Validate the data structure has all required columns."""
        required_columns = {
            "Trade date",
            "Settle date",
            "Reference",
            "Description",
            "Unit cost (p)",
            "Quantity",
            "Value (£)",
        }

        if not self.data:
            raise ValueError("No data provided")

        # HL data starts on row 6...
        actual_columns = set(self.data[5])
        missing_columns = required_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return self

    def get_storage_path(self) -> str:
        """Get the storage path for the transaction data."""
        date_str = self.timestamp.strftime("%Y%m%d")
        return (
            f"transactions/hargreaves_lansdown/{self.portfolio_name}/{date_str}/transactions.csv.gz"
        )


class HargreavesLansdownPosition(HargreavesLansdownBase):
    """Model for Hargreaves Lansdown current positions."""

    @model_validator(mode="after")
    def validate_data_structure(self) -> "HargreavesLansdownPosition":
        """Validate the data structure has all required columns."""
        required_columns = {
            "Stock",
            "Units held",
            "Price (pence)",
            "Value (£)",
            "Cost (£)",
            "Gain/loss (£)",
            "Gain/loss (%)",
            "Code",
        }

        if not self.data:
            raise ValueError("No data provided")

        # HL data starts on row 11...
        actual_columns = set(self.data[10])
        missing_columns = required_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return self

    def get_storage_path(self) -> str:
        """Get the storage path for the current positions data."""
        date_str = self.timestamp.strftime("%Y%m%d")
        return f"transactions/hargreaves_lansdown/{self.portfolio_name}/{date_str}/positions.csv.gz"


class HargreavesLansdownClosedPosition(HargreavesLansdownBase):
    """Model for Hargreaves Lansdown closed positions."""

    @model_validator(mode="after")
    def validate_data_structure(self) -> "HargreavesLansdownClosedPosition":
        """Validate the data structure has all required columns."""
        required_columns = {
            "Code",
            "Stock",
            "Disposal type",
            "Disposal date",
        }

        if not self.data:
            raise ValueError("No data provided")

        # HL data starts on row 6...
        actual_columns = set(self.data[5])
        missing_columns = required_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return self

    def get_storage_path(self) -> str:
        """Get the storage path for the closed positions data."""
        date_str = self.timestamp.strftime("%Y%m%d")
        return f"transactions/hargreaves_lansdown/{self.portfolio_name}/{date_str}/closed_positions.csv.gz"
