import gzip
import json
from unittest.mock import MagicMock, patch

import pytest

from src.common.gcp.client import GCPStorageClient
from src.common.types import JSONType


@pytest.fixture
def reset_singleton() -> None:
    """Reset the singleton instance between tests."""
    GCPStorageClient._instance = None
    GCPStorageClient._client = None


class TestGCPStorageClient:
    def test_singleton_pattern(self, reset_singleton: None) -> None:
        """Test that the GCPStorageClient follows the singleton pattern."""
        client1 = GCPStorageClient()
        client2 = GCPStorageClient()
        assert client1 is client2

    @patch("google.cloud.storage.Client")
    def test_client_initialisation(
        self, mock_storage_client: MagicMock, reset_singleton: None
    ) -> None:
        """Test that the storage client is initialised correctly."""
        client = GCPStorageClient()
        mock_storage_client.assert_called_once()
        assert client._client is not None

    @patch("google.cloud.storage.Client")
    def test_store_json_data_compressed(
        self, mock_storage_client: MagicMock, reset_singleton: None
    ) -> None:
        """Test storing compressed JSON data."""
        # Setup
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Test data
        test_data: JSONType = {"key": "value"}
        bucket_name = "test-bucket"
        blob_path = "test/path.json"

        # Execute
        client = GCPStorageClient()
        client.store_json_data(test_data, bucket_name, blob_path, compress=True)

        # Verify
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(blob_path)

        # Verify compression and content type settings
        assert mock_blob.content_encoding == "gzip"
        assert mock_blob.content_type == "text/plain"

        # Verify the uploaded content is compressed
        expected_content = gzip.compress(json.dumps(test_data).encode("utf-8"))
        mock_blob.upload_from_string.assert_called_once()
        actual_content = mock_blob.upload_from_string.call_args[0][0]
        assert actual_content == expected_content

    @patch("google.cloud.storage.Client")
    def test_store_json_data_uncompressed(
        self, mock_storage_client: MagicMock, reset_singleton: None
    ) -> None:
        """Test storing uncompressed JSON data."""
        # Setup
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_storage_client.return_value.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob

        # Test data
        test_data: JSONType = {"key": "value"}
        bucket_name = "test-bucket"
        blob_path = "test/path.json"

        # Execute
        client = GCPStorageClient()
        client.store_json_data(test_data, bucket_name, blob_path, compress=False)

        # Verify
        mock_storage_client.return_value.bucket.assert_called_once_with(bucket_name)
        mock_bucket.blob.assert_called_once_with(blob_path)

        # Verify content type settings
        assert mock_blob.content_type == "application/json"

        # Verify the uploaded content is not compressed
        expected_content = json.dumps(test_data).encode("utf-8")
        mock_blob.upload_from_string.assert_called_once()
        actual_content = mock_blob.upload_from_string.call_args[0][0]
        assert actual_content == expected_content

    def test_store_json_data_without_initialisation(self, reset_singleton: None) -> None:
        """Test that attempting to store data without initialisation raises an error."""
        client = GCPStorageClient()
        client._client = None  # Force uninitialized state

        with pytest.raises(RuntimeError, match="GCP Storage client not initialised"):
            client.store_json_data({"key": "value"}, "bucket", "path.json")
