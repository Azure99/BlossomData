from typing import Any, Optional

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
        with open(config_file, encoding="utf-8") as file:
            data = yaml.safe_load(file)
        return Config(**data)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"The file at {config_file} was not found.") from e
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}") from e
    except ValidationError as e:
        raise ValueError(f"Error validating configuration data: {e}") from e
