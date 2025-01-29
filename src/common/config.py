import os
from typing import Any

import yaml


def load_yaml_config(path: str) -> dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)  # type: ignore # FIXME


def resolve_env_vars(config: dict[str, Any]) -> dict[str, Any]:
    """Recursively resolve environment variables in config."""
    resolved: dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = resolve_env_vars(value)
        elif isinstance(value, str) and value.startswith("${") and value.endswith("}"):
            env_var = value[2:-1]
            env_value = os.environ.get(env_var)
            if env_value is None:
                raise ValueError(f"Environment variable {env_var} not set")
            resolved[key] = env_value
        else:
            resolved[key] = value
    return resolved
