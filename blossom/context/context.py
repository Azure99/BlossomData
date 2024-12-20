from typing import Any, Optional

from blossom.conf.config import Config
from blossom.context.provider_manager import ProviderManager
from blossom.provider.base_provider import BaseProvider
from blossom.provider.protocol import ChatCompletionResponse
from blossom.schema.chat_schema import ChatMessage, user, assistant


class Context:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.provider_manager = ProviderManager(config=config)

    def get_config(self) -> Config:
        return self.config

    def get_provider_manager(self) -> ProviderManager:
        return self.provider_manager

    def get_model(self, model_name: str) -> BaseProvider:
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

    def chat_completion_with_messages(
        self,
        model: str,
        messages: list[ChatMessage],
        extra_params: Optional[dict[str, Any]] = None,
    ) -> list[ChatMessage]:
        response = self.chat_completion(model, messages, extra_params=extra_params)
        return messages + [assistant(response)]

    def single_chat_completion(
        self,
        model: str,
        user_message: str,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> str:
        return self.chat_completion(
            model=model,
            messages=[user(user_message)],
            extra_params=extra_params,
        )

    def single_chat_completion_with_details(
        self,
        model: str,
        user_message: str,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> ChatCompletionResponse:
        return self.chat_completion_with_details(
            model=model,
            messages=[user(user_message)],
            extra_params=extra_params,
        )
