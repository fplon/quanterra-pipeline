import json
import os
import subprocess

from packaging import version

from src.clients.google_cloud_storage_client import GCPStorageClient


class CLIToolUpdater:
    def __init__(self, bucket_name: str) -> None:
        self.client = GCPStorageClient()
        self.bucket = self.client._client.bucket(bucket_name)
        self.install_path = os.path.expanduser("~/.quanterra-cli")

    def get_latest_version(self) -> str:
        blob = self.bucket.blob("latest_version.json")
        version_info = json.loads(blob.download_as_string())
        return version_info["version"]

    def check_for_updates(self) -> None:
        try:
            current_version = self._get_current_version()
            latest_version = self.get_latest_version()

            if version.parse(latest_version) > version.parse(current_version):
                print(f"New version {latest_version} available. Current version: {current_version}")
                if input("Do you want to update? [y/N]: ").lower() == "y":
                    self._perform_update(latest_version)
        except Exception as e:
            print(f"Failed to check for updates: {e}")

    def _get_current_version(self) -> str:
        version_file = os.path.join(self.install_path, "version.txt")
        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                return f.read().strip()
        return "0.0.0"

    def _perform_update(self, new_version: str) -> None:
        """Update the CLI tool to the latest version.

        Args:
            new_version: The version to update to
        """
        try:
            os.makedirs(self.install_path, exist_ok=True)

            # Download from GCS
            project_file = f"quanterra_pipeline-{new_version}.tar.gz"
            package_blob = self.bucket.blob(f"versions/{project_file}")
            package_path = os.path.join(self.install_path, project_file)
            package_blob.download_to_filename(package_path)

            # Install in a virtual environment
            venv_path = os.path.join(self.install_path, "venv")
            subprocess.check_call(["uv", "venv", "--python", "3.12", venv_path])
            subprocess.check_call(
                [
                    "uv",
                    "pip",
                    "install",
                    "--python",
                    os.path.join(venv_path, "bin", "python"),
                    "--force-reinstall",
                    package_path,
                ]
            )

            # Update the version file
            version_file = os.path.join(self.install_path, "version.txt")
            with open(version_file, "w") as f:
                f.write(new_version)

            # Create symlink to make CLI available globally
            cli_script = os.path.join(venv_path, "bin", "quanterra-cli")
            symlink_path = os.path.expanduser("~/.local/bin/quanterra-cli")
            os.makedirs(os.path.dirname(symlink_path), exist_ok=True)

            if os.path.exists(symlink_path):
                os.remove(symlink_path)

            os.symlink(cli_script, symlink_path)

            # Print installation location for debugging
            print(f"Successfully updated to version {new_version}")
            print(f"Installation path: {self.install_path}")
            print(f"CLI available at: {symlink_path}")

            # Clean up
            os.remove(package_path)

        except Exception as e:
            raise Exception(f"Failed to perform update: {str(e)}")
