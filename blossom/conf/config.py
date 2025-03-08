from typing import Any, Optional

import yaml
from pydantic import BaseModel, ValidationError

DEFAULT_CONFIG_FILE = "config.yaml"


class ModelConfig(BaseModel):
    name: str
    provider: str
    api_model_name: Optional[str]
    config: dict[str, Any]
    extra_params: Optional[dict[str, Any]] = None


class Config(BaseModel):
    models: list[ModelConfig]


def load_config(config_file: str = DEFAULT_CONFIG_FILE) -> Config:
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
