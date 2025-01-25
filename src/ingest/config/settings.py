import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class GCPSettings:
    """Google Cloud Platform settings."""

    project_id: str
    bucket_name: str
    credentials_path: Path


@dataclass
class EODHDSettings:
    """EODHD provider settings."""

    api_key: str
    base_url: str
    exchanges: list[str]
    instruments: list[str]
    economic_events: dict[str, list[str] | str]
    macro_indicators: dict[str, list[str]]
    asset_types: list[str]
    include_delisted: bool = False


@dataclass
class Settings:
    """Application settings."""

    env: str
    gcp: GCPSettings
    eodhd: EODHDSettings

    @classmethod
    def load(cls, env: str = "dev") -> "Settings":
        """Load settings from config file and environment variables."""
        # Load config file
        config_path = Path(__file__).parent / f"{env}.yml"
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        # Create GCP settings
        gcp_config = config.get("gcp", {})
        gcp = GCPSettings(
            project_id=os.getenv("GCP_PROJECT_ID", gcp_config.get("project_id", "")),
            bucket_name=os.getenv("GCP_BUCKET_NAME", gcp_config.get("bucket_name", "")),
            credentials_path=Path(
                os.getenv(
                    "GCP_CREDENTIALS_PATH",
                    str(gcp_config.get("credentials_path", "")),
                )
            ).expanduser(),
        )

        # Create EODHD settings with environment variable overrides
        eodhd_config = config.get("providers", {}).get("eodhd", {})
        eodhd = EODHDSettings(
            api_key=os.getenv("EODHD_API_KEY", eodhd_config.get("api_key", "")),
            base_url=os.getenv("EODHD_BASE_URL", eodhd_config.get("base_url", "")),
            exchanges=eodhd_config.get("exchanges", []),
            instruments=eodhd_config.get("instruments", []),
            economic_events=eodhd_config.get(
                "economic_events", {"countries": [], "comparison": "previous"}
            ),
            macro_indicators=eodhd_config.get(
                "macro_indicators", {"countries": [], "indicators": []}
            ),
            asset_types=eodhd_config.get("asset_types", []),
            include_delisted=eodhd_config.get("include_delisted", False),
        )

        return cls(env=env, gcp=gcp, eodhd=eodhd)

    # TODO this should be a method on the provider service
    def get_provider_instruments(self, provider: str) -> list[tuple[str, str]]:
        """Get list of exchange/instrument pairs for a provider."""
        provider_config = getattr(self, provider)
        return [tuple(instrument.split(".", 1)) for instrument in provider_config.instruments]

    # TODO this should be a method on the provider service
    def get_provider_exchanges(self, provider: str) -> list[str]:
        """Get list of exchanges for a provider."""
        provider_config = getattr(self, provider)
        return [exchange for exchange in provider_config.exchanges]


# Global settings instance
_settings: Settings | None = None


def get_settings(env: str = "dev") -> Settings:
    """Get application settings, loading them if necessary."""
    global _settings
    if _settings is None:
        _settings = Settings.load(env)
    return _settings


def reload_settings(env: str = "dev") -> Settings:
    """Force reload settings."""
    global _settings
    _settings = Settings.load(env)
    return _settings
