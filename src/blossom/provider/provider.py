from abc import ABC, abstractmethod
from typing import Any, Optional

from blossom.conf.config import ModelConfig
from blossom.provider.protocol import ChatCompletionResponse, EmbeddingResponse
from blossom.schema.chat_schema import ChatMessage


class Provider(ABC):
    def __init__(self, model_config: ModelConfig):
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
        pass

    @abstractmethod
    def chat_completion_with_details(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> ChatCompletionResponse:
        pass

    @abstractmethod
    def embedding(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> list[float]:
        pass

    @abstractmethod
    def embedding_with_details(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> EmbeddingResponse:
        pass
