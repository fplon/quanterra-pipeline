import os
from unittest.mock import patch

import pytest

from src.cli.tool_update import CLIToolUpdater


@pytest.fixture
def mock_gcp_storage_client():
    with patch("src.cli.tool_update.GCPStorageClient") as mock_client:
        yield mock_client


@pytest.fixture
def tool_updater():
    return CLIToolUpdater("datalake-dev-cli-tool-config")


@pytest.fixture
def new_version():
    return "0.1.0"


class TestCLIToolUpdater:
    def test_initialisation(self, tool_updater) -> None:
        assert tool_updater.bucket.name == "datalake-dev-cli-tool-config"
        assert tool_updater.install_path == os.path.expanduser("~/.quanterra-cli")

    def test_get_latest_version(self, tool_updater) -> None:
        version = tool_updater.get_latest_version()
        assert isinstance(version, str)
        assert version == "0.1.0"  # TODO regex this

    def test__get_current_version(self, tool_updater) -> None:
        current_version = tool_updater._get_current_version()
        assert isinstance(current_version, str)
        assert current_version == "0.1.0"

    def test__perform_update(self, tool_updater, new_version) -> None:
        tool_updater._perform_update(new_version)
        assert True

    def test_check_for_updates(self, tool_updater) -> None:
        tool_updater.check_for_updates()
        assert True
