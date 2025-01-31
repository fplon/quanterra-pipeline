from datetime import datetime

from pydantic import BaseModel, Field, model_validator


class InteractiveInvestorConfig(BaseModel):
    """Configuration for Interactive Investor data ingestion."""

    bucket_name: str
    source_path: str


class InteractiveInvestorTransaction(BaseModel):
    """Model for Interactive Investor transaction data."""

    data: list[list[str]]
    timestamp: datetime = Field(default_factory=datetime.now)

    @model_validator(mode="after")
    def validate_data_structure(self) -> "InteractiveInvestorTransaction":
        """Validate the data structure has all required columns."""
        required_columns = {
            "Date",
            "Settlement Date",
            "Symbol",
            "Sedol",
            "Quantity",
            "Price",
            "Description",
            "Reference",
            "Debit",
            "Credit",
            "Running Balance",
        }

        if not self.data:
            raise ValueError("No data provided")

        actual_columns = set(self.data[0])
        missing_columns = required_columns - actual_columns
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return self

    def get_storage_path(self) -> str:
        """Get the storage path for the transaction data."""
        date_str = self.timestamp.strftime("%Y%m%d")
        return f"transactions/interactive_investor/{date_str}.csv.gz"
