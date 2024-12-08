import time
from typing import Any, Optional

import requests
from blossom.log import logger

from blossom.conf import ModelConfig
from blossom.provider.base_provider import BaseProvider
from blossom.schema.chat_schema import ChatMessage, ChatRole
from blossom.util.json import json_dumps

DEFAULT_BASE_URL = "https://api.openai.com/v1"

MAX_TOO_MANY_REQUESTS_RETRIES = 10
TOO_MANY_REQUESTS_BACKOFF_FACTOR = 1.5


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

        data = {
            "model": self.api_model_name,
            "messages": [
                {"role": message.role, "content": message.content}
                for message in messages
            ],
        }

        response = self._request("/chat/completions", data, extra_params)

        content = response["choices"][0]["message"]["content"]
        assert isinstance(content, str)
        return content

    def embedding(
        self, input: str, extra_params: Optional[dict[str, Any]]
    ) -> list[float]:
        if len(input) == 0:
            raise ValueError("No input provided")

        data = {"model": self.api_model_name, "input": input}

        response = self._request("/embeddings", data, extra_params)
        embedding = response["data"][0]["embedding"]
        assert isinstance(embedding, list)
        return embedding

    def _request(
        self,
        url_part: str,
        data: dict[str, Any],
        extra_params: Optional[dict[str, Any]],
    ) -> Any:
        url = f"{self.base_url}{url_part}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.extra_params is not None:
            data.update(self.extra_params)
        if extra_params is not None:
            data.update(extra_params)

        rate_limit_retry = 0
        rate_limit_backoff = 1.0

        while rate_limit_retry < MAX_TOO_MANY_REQUESTS_RETRIES:
            logger.info(f"Sending request to OpenAI: {url}, {data}")
            response = requests.post(
                url,
                timeout=600,
                headers=headers,
                data=json_dumps(data, ensure_ascii=True),
            )
            logger.info(f"OpenAI response: {response.text}")

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429 or response.status_code >= 500:
                logger.warning(
                    f"Rate limit exceeded, retrying in {rate_limit_backoff} seconds"
                )
                time.sleep(rate_limit_backoff)
                rate_limit_backoff *= TOO_MANY_REQUESTS_BACKOFF_FACTOR
                rate_limit_retry += 1
            else:
                break

        raise ValueError(
            f"Request failed with status code {response.status_code}, {response.text}"
        )
