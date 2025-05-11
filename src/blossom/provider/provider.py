from abc import ABC, abstractmethod
from typing import Any, Optional

from blossom.conf.config import ModelConfig
from blossom.provider.protocol import ChatCompletionResponse, EmbeddingResponse
from blossom.schema.chat_schema import ChatMessage


class Provider(ABC):
    """
    Abstract base class for model providers.

    A Provider is responsible for interfacing with a specific AI model service,
    such as OpenAI, Anthropic, etc. It handles the details of API communication
    and provides a consistent interface for model interactions.
    """

    def __init__(self, model_config: ModelConfig):
        """
        Initialize a Provider with model configuration.

        Args:
            model_config: Configuration for the model
        """
        self.api_model_name = (
            model_config.api_model_name
            if model_config.api_model_name
            else model_config.name
        )
        self.config = model_config.config
        self.extra_params = model_config.extra_params

    @abstractmethod
    def chat_completion(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        """
        Generate a chat completion.

        Args:
            messages: List of chat messages for the conversation
            extra_params: Optional additional parameters for the API call

        Returns:
            Generated text response
        """
        pass

    @abstractmethod
    def chat_completion_with_details(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> ChatCompletionResponse:
        """
        Generate a chat completion with detailed response information.

        Args:
            messages: List of chat messages for the conversation
            extra_params: Optional additional parameters for the API call

        Returns:
            Detailed response including choices and usage information
        """
        pass

    @abstractmethod
    def embedding(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> list[float]:
        """
        Generate embeddings for the input text.

        Args:
            input_text: Text to generate embeddings for
            extra_params: Optional additional parameters for the API call

        Returns:
            Vector embedding as a list of floats
        """
        pass

    @abstractmethod
    def embedding_with_details(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> EmbeddingResponse:
        """
        Generate embeddings with detailed response information.

        Args:
            input_text: Text to generate embeddings for
            extra_params: Optional additional parameters for the API call

        Returns:
            Detailed response including embedding data and usage information
        """
        pass
