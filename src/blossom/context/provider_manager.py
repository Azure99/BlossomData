import importlib
import inspect
import pkgutil
from typing import Type

from blossom.conf.config import Config, ModelConfig
from blossom.provider.provider import Provider

PROVIDER_PACKAGE = "blossom.provider"


class ProviderManager:
    """
    Manager for model providers.

    This class is responsible for dynamically loading provider implementations
    and creating provider instances based on configuration.
    """

    def __init__(self, config: Config):
        """
        Initialize a ProviderManager with configuration.

        Args:
            config: Configuration containing model definitions
        """
        self.config = config
        self.providers = self._load_providers()
        self.models = self._load_models()
        self.provider_instances: dict[str, Provider] = {}

    def get_model(self, model_name: str) -> Provider:
        """
        Get a provider instance for a model by name.

        This method creates and caches provider instances as needed.

        Args:
            model_name: Name of the model to get

        Returns:
            Provider instance for the specified model

        Raises:
            ValueError: If the model is not found in configuration or the provider is not found
        """
        if model_name in self.provider_instances:
            return self.provider_instances[model_name]

        model_config = self.models.get(model_name)
        if not model_config:
            raise ValueError(f"Model {model_name} not found in configuration")

        provider_class = self.providers.get(model_config.provider)
        if not provider_class:
            raise ValueError(f"Provider {model_config.provider} not found")

        provider_instance = provider_class(model_config)
        self.provider_instances[model_name] = provider_instance
        return provider_instance

    def _load_models(self) -> dict[str, ModelConfig]:
        models = {}
        for model in self.config.models:
            models[model.name] = model
        return models

    @staticmethod
    def _load_providers() -> dict[str, Type[Provider]]:
        providers = {}
        for module_info in pkgutil.walk_packages(
            importlib.import_module(PROVIDER_PACKAGE).__path__, f"{PROVIDER_PACKAGE}."
        ):
            module = importlib.import_module(module_info.name)
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, Provider) and obj is not Provider:
                    providers[name.lower()] = obj
        return providers
