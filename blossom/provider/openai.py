from typing import Any, Optional

import requests

from blossom.conf import ModelConfig
from blossom.provider.base_provider import BaseProvider
from blossom.schema.chat_schema import ChatMessage, ChatRole
from blossom.util.json import json_dumps

DEFAULT_BASE_URL = "https://api.openai.com/v1"


class OpenAI(BaseProvider):
    def __init__(self, model_config: ModelConfig):
        super().__init__(model_config)
        self.base_url = model_config.config.get("base_url", DEFAULT_BASE_URL)
        self.api_key = model_config.config["key"]
        self.default_system = model_config.config.get("default_system", None)

    def chat_completion(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        if len(messages) == 0:
            raise ValueError("No messages provided")

        if messages[0].role != ChatRole.SYSTEM and self.default_system:
            system_message = ChatMessage(
                role=ChatRole.SYSTEM, content=self.default_system
            )
            messages = [system_message] + messages

        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        data = {
            "model": self.api_model_name,
            "messages": [
                {"role": message.role, "content": message.content}
                for message in messages
            ],
        }
        if self.extra_params is not None:
            data.update(self.extra_params)
        if extra_params is not None:
            data.update(extra_params)

        response = requests.post(
            url, timeout=600, headers=headers, data=json_dumps(data, ensure_ascii=True)
        )

        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            raise ValueError(
                f"Request failed with status code {response.status_code}, {response.text}"
            )
