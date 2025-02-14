from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from src.clients.file.base_csv_client import BaseCSVFileClient


@pytest.fixture
def client() -> BaseCSVFileClient:
    """Create a BaseCSVFileClient instance with default settings."""
    return BaseCSVFileClient()


@pytest.fixture
def custom_client() -> BaseCSVFileClient:
    """Create a BaseCSVFileClient instance with custom settings."""
    return BaseCSVFileClient(
        delimiter=";",
        encoding="utf-8",
        preview_rows=5,
    )


class TestBaseCSVFileClient:
    """Test suite for BaseCSVFileClient."""

    def test_initialisation_default_values(self, client: BaseCSVFileClient) -> None:
        """Test client initialisation with default values."""
        assert client.delimiter == ","
        assert client.encoding == "ISO-8859-1"
        assert client.preview_rows == 15

    def test_initialisation_custom_values(self, custom_client: BaseCSVFileClient) -> None:
        """Test client initialisation with custom values."""
        assert custom_client.delimiter == ";"
        assert custom_client.encoding == "utf-8"
        assert custom_client.preview_rows == 5

    def test_validate_file_type_valid_csv(self, client: BaseCSVFileClient) -> None:
        """Test validation of valid CSV file paths."""
        # Test with string path
        assert client.validate_file_type("test.csv") is True
        assert client.validate_file_type("path/to/test.CSV") is True

        # Test with Path object
        assert client.validate_file_type(Path("test.csv")) is True
        assert client.validate_file_type(Path("path/to/test.CSV")) is True

    def test_validate_file_type_invalid_extensions(self, client: BaseCSVFileClient) -> None:
        """Test validation of invalid file extensions."""
        invalid_extensions = [
            "test.txt",
            "test.xlsx",
            "test",
            "test.csv.txt",
            ".csv.hidden",
        ]

        for file_path in invalid_extensions:
            assert client.validate_file_type(file_path) is False
            assert client.validate_file_type(Path(file_path)) is False

    def test_preview_file_with_default_settings(self, client: BaseCSVFileClient) -> None:
        """Test preview_file with default settings."""
        mock_csv_content = "a,b,c\n1,2,3\n4,5,6"
        mock_file = mock_open(read_data=mock_csv_content)

        with patch("builtins.open", mock_file):
            result = client.preview_file("test.csv")

        assert len(result) == 3
        assert result == [["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]]
        mock_file.assert_called_once_with("test.csv", "r", encoding="ISO-8859-1")

    def test_preview_file_with_custom_settings(self, custom_client: BaseCSVFileClient) -> None:
        """Test preview_file with custom delimiter and encoding."""
        mock_csv_content = "a;b;c\n1;2;3\n4;5;6"
        mock_file = mock_open(read_data=mock_csv_content)

        with patch("builtins.open", mock_file):
            result = custom_client.preview_file("test.csv")

        assert len(result) == 3
        assert result == [["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]]
        mock_file.assert_called_once_with("test.csv", "r", encoding="utf-8")

    def test_preview_file_respects_preview_rows_limit(self, client: BaseCSVFileClient) -> None:
        """Test preview_file respects the preview_rows limit."""
        # Create CSV content with more rows than preview_rows
        csv_rows = [f"{i},{i + 1},{i + 2}" for i in range(20)]
        mock_csv_content = "\n".join(csv_rows)
        mock_file = mock_open(read_data=mock_csv_content)

        with patch("builtins.open", mock_file):
            result = client.preview_file("test.csv")

        assert len(result) == client.preview_rows  # Should only return preview_rows number of rows

    def test_preview_file_with_empty_file(self, client: BaseCSVFileClient) -> None:
        """Test preview_file with an empty file."""
        mock_file = mock_open(read_data="")

        with patch("builtins.open", mock_file):
            result = client.preview_file("test.csv")

        assert result == []

    def test_preview_file_with_path_object(self, client: BaseCSVFileClient) -> None:
        """Test preview_file accepts Path objects."""
        mock_csv_content = "a,b,c\n1,2,3"
        mock_file = mock_open(read_data=mock_csv_content)

        with patch("builtins.open", mock_file):
            result = client.preview_file(Path("test.csv"))

        assert len(result) == 2
        assert result == [["a", "b", "c"], ["1", "2", "3"]]

    @pytest.mark.parametrize(
        "mock_content,expected_result",
        [
            ("a,b,c\n1,2,3", [["a", "b", "c"], ["1", "2", "3"]]),  # Normal case
            ("a,b,c\n1,2", [["a", "b", "c"], ["1", "2"]]),  # Missing value
            ("a,b,c\n1,2,3,4", [["a", "b", "c"], ["1", "2", "3", "4"]]),  # Extra value
            ('a,"b,c",d\n1,"2,3",4', [["a", "b,c", "d"], ["1", "2,3", "4"]]),  # Quoted values
            ("", []),  # Empty file
        ],
    )
    def test_preview_file_various_content_formats(
        self,
        client: BaseCSVFileClient,
        mock_content: str,
        expected_result: list[list[str]],
    ) -> None:
        """Test preview_file with various CSV content formats."""
        mock_file = mock_open(read_data=mock_content)

        with patch("builtins.open", mock_file):
            result = client.preview_file("test.csv")

        assert result == expected_result
