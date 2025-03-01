from typing import Any, Optional

from blossom.conf.config import ModelConfig
from blossom.provider.protocol import ChatCompletionResponse
from blossom.schema.chat_schema import ChatMessage


class Provider:
    def __init__(self, model_config: ModelConfig):
        self.api_model_name = (
            model_config.api_model_name
            if model_config.api_model_name
            else model_config.name
        )
        self.config = model_config.config
        self.extra_params = model_config.extra_params

    def chat_completion(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        raise NotImplementedError("Service not implemented")

    def chat_completion_with_details(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> ChatCompletionResponse:
        raise NotImplementedError("Service not implemented")

    def embedding(
        self, input_text: str, extra_params: Optional[dict[str, Any]]
    ) -> list[float]:
        raise NotImplementedError("Service not implemented")
