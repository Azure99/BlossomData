from typing import Any, Optional

from blossom.conf.config import Config, load_config
from blossom.context.provider_manager import ProviderManager
from blossom.provider.protocol import ChatCompletionResponse
from blossom.provider.provider import Provider
from blossom.schema.chat_schema import ChatMessage


class Context:
    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or load_config()
        self.provider_manager = ProviderManager(config=self.config)

    def get_config(self) -> Config:
        return self.config

    def get_provider_manager(self) -> ProviderManager:
        return self.provider_manager

    def get_model(self, model_name: str) -> Provider:
        return self.provider_manager.get_model(model_name)

    def chat_completion(
        self,
        model: str,
        messages: list[ChatMessage],
        extra_params: Optional[dict[str, Any]] = None,
    ) -> str:
        return self.get_model(model).chat_completion(
            messages, extra_params=extra_params
        )

    def chat_completion_with_details(
        self,
        model: str,
        messages: list[ChatMessage],
        extra_params: Optional[dict[str, Any]] = None,
    ) -> ChatCompletionResponse:
        return self.get_model(model).chat_completion_with_details(
            messages, extra_params=extra_params
        )
