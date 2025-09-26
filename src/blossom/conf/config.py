from typing import Any, Optional
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, ValidationError

DEFAULT_CONFIG_FILE = "config.yaml"


class ModelConfig(BaseModel):
    """
    Configuration for a model.

    This class defines the configuration for a specific model,
    including its name, provider, and provider-specific configuration.

    Attributes:
        name: The name of the model as referenced in the application
        provider: The provider class name (e.g., "openai")
        api_model_name: Optional name to use when calling the provider API
        config: Provider-specific configuration parameters
        extra_params: Optional additional parameters for API calls
    """

    name: str
    provider: str
    api_model_name: Optional[str] = None
    config: dict[str, Any]
    extra_params: Optional[dict[str, Any]] = None


class Config(BaseModel):
    """
    Global configuration.

    This class represents the top-level configuration, containing
    settings for all the models used in the application.

    Attributes:
        models: List of model configurations
    """

    models: list[ModelConfig]


def _discover_config_path(config_file: str = DEFAULT_CONFIG_FILE) -> Path:
    """
    Discover a valid configuration file path.

    Search order (first existing wins):
    1) Env var `BLOSSOM_CONFIG`
    2) Explicit `config_file` path (relative to CWD or absolute)
    3) User config: `~/.blossom.yaml`

    Args:
        config_file: Preferred filename (default: "config.yaml")

    Returns:
        Path to the existing configuration file

    Raises:
        FileNotFoundError: If none of the candidate paths exist
    """
    candidates: list[Path] = []

    # 1) Honor the BLOSSOM_CONFIG env var so callers can override discovery.
    env_path = os.environ.get("BLOSSOM_CONFIG")
    if env_path:
        candidates.append(Path(os.path.expanduser(env_path)))

    # 2) Try the provided config_file path (absolute, or relative to the CWD).
    p = Path(config_file)
    candidates.append(p if p.is_absolute() else Path.cwd() / p)

    # 3) Fall back to the per-user default ~/.blossom.yaml.
    home = Path.home()
    candidates.append(home / ".blossom.yaml")

    for c in candidates:
        if c.exists() and c.is_file():
            return c

    # Exhausted all candidates without finding a file; surface the search list.
    search_list = "\n".join(str(c) for c in candidates)
    raise FileNotFoundError("No configuration file found. Tried:\n" + search_list)


def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> Config:
    """
    Load configuration from a YAML file.

    Args:
        config_file: Path to the configuration file (default: "config.yaml")

    Returns:
        Parsed configuration object

    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        ValueError: If there's an error parsing the YAML or validating the configuration
    """
    try:
        cfg_path = _discover_config_path(config_file)
        with open(cfg_path, encoding="utf-8") as file:
            data = yaml.safe_load(file)
        return Config(**data)
    except FileNotFoundError as e:
        raise FileNotFoundError(str(e)) from e
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}") from e
    except ValidationError as e:
        raise ValueError(f"Error validating configuration data: {e}") from e
