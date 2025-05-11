from typing import Any, Optional

from blossom.conf.config import Config, load_config
from blossom.context.provider_manager import ProviderManager
from blossom.provider.protocol import ChatCompletionResponse, EmbeddingResponse
from blossom.provider.provider import Provider
from blossom.schema.chat_schema import ChatMessage


class Context:
    """
    Execution context for operations.

    The Context class provides access to configuration and model providers,
    and offers convenience methods for interacting with models.
    It serves as the central point for accessing resources needed by operators.
    """

    def __init__(self, config: Optional[Config] = None) -> None:
        """
        Initialize a Context with optional configuration.

        Args:
            config: Configuration object (if None, loads from default config file)
        """
        self.config = config or load_config()
        self.provider_manager = ProviderManager(config=self.config)

    def get_config(self) -> Config:
        """
        Get the configuration.

        Returns:
            The current configuration
        """
        return self.config

    def get_provider_manager(self) -> ProviderManager:
        """
        Get the provider manager.

        Returns:
            The provider manager instance
        """
        return self.provider_manager

    def get_model(self, model_name: str) -> Provider:
        """
        Get a model provider by name.

        Args:
            model_name: Name of the model to get

        Returns:
            Provider instance for the specified model

        Raises:
            ValueError: If the model is not found in configuration
        """
        return self.provider_manager.get_model(model_name)

    def chat_completion(
        self,
        model: str,
        messages: list[ChatMessage],
        extra_params: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        Generate a chat completion using the specified model.

        Args:
            model: Name of the model to use
            messages: List of chat messages for the conversation
            extra_params: Optional additional parameters for the API call

        Returns:
            Generated text response
        """
        return self.get_model(model).chat_completion(
            messages, extra_params=extra_params
        )

    def chat_completion_with_details(
        self,
        model: str,
        messages: list[ChatMessage],
        extra_params: Optional[dict[str, Any]] = None,
    ) -> ChatCompletionResponse:
        """
        Generate a chat completion with detailed response information.

        Args:
            model: Name of the model to use
            messages: List of chat messages for the conversation
            extra_params: Optional additional parameters for the API call

        Returns:
            Detailed response including choices and usage information
        """
        return self.get_model(model).chat_completion_with_details(
            messages, extra_params=extra_params
        )

    def embedding(
        self,
        model: str,
        input_text: str,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> list[float]:
        """
        Generate embeddings for the input text using the specified model.

        Args:
            model: Name of the model to use
            input_text: Text to generate embeddings for
            extra_params: Optional additional parameters for the API call

        Returns:
            Vector embedding as a list of floats
        """
        return self.get_model(model).embedding(input_text, extra_params=extra_params)

    def embedding_with_details(
        self,
        model: str,
        input_text: str,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> EmbeddingResponse:
        """
        Generate embeddings with detailed response information.

        Args:
            model: Name of the model to use
            input_text: Text to generate embeddings for
            extra_params: Optional additional parameters for the API call

        Returns:
            Detailed response including embedding data and usage information
        """
        return self.get_model(model).embedding_with_details(
            input_text, extra_params=extra_params
        )
