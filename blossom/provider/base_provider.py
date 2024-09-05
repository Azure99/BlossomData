from typing import Any, Optional

from blossom.conf.config import ModelConfig
from blossom.schema.chat_schema import ChatMessage


class BaseProvider:
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
