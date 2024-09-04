from typing import Any, Optional

import requests

from blossom.conf import ModelConfig
from blossom.provider.base_provider import BaseProvider
from blossom.schema.chat_schema import ChatMessage
from blossom.util.json import json_dumps


class OpenAI(BaseProvider):
    def __init__(self, model_config: ModelConfig):
        super().__init__(model_config)
        self.base_url = model_config.auth["base_url"]
        self.api_key = model_config.auth["key"]

    def chat_completion(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        data = {
            "model": self.model_name,
            "messages": [
                {"role": message.role, "content": message.content}
                for message in messages
            ],
        }
        if self.extra_params is not None:
            data.update(self.extra_params)
        if extra_params is not None:
            data.update(extra_params)

        response = requests.post(url, headers=headers, data=json_dumps(data))

        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            raise ValueError(
                f"Request failed with status code {response.status_code}, {response.text}"
            )
