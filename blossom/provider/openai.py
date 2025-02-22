import random
import time
from typing import Any, Optional

import requests

from blossom.conf import ModelConfig
from blossom.log import logger
from blossom.provider.base_provider import BaseProvider
from blossom.provider.protocol import ChatCompletionResponse
from blossom.schema.chat_schema import ChatMessage, ChatRole, system
from blossom.util.json import json_dumps

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT = 600

MAX_TOO_MANY_REQUESTS_RETRIES = 12
TOO_MANY_REQUESTS_BACKOFF_FACTOR = 1.5


class OpenAI(BaseProvider):
    def __init__(self, model_config: ModelConfig):
        super().__init__(model_config)
        self.base_url = model_config.config.get("base_url", DEFAULT_BASE_URL)
        self.timeout = model_config.config.get("timeout", DEFAULT_TIMEOUT)
        self.default_system = model_config.config.get("default_system", None)
        self.api_keys = self._load_api_keys(model_config)

    @staticmethod
    def _load_api_keys(model_config: ModelConfig) -> list[str]:
        api_keys = []
        if "key" in model_config.config:
            api_keys.append(model_config.config["key"])
        if "keys" in model_config.config:
            api_keys.extend(model_config.config["keys"])
        if len(api_keys) == 0:
            raise ValueError("No API key provided")
        if not all(isinstance(key, str) for key in api_keys):
            raise ValueError("API keys must be strings")
        return api_keys

    def chat_completion(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> str:
        response = self.chat_completion_with_details(messages, extra_params)
        content = response.choices[0].message.content
        assert isinstance(content, str)
        return content

    def chat_completion_with_details(
        self, messages: list[ChatMessage], extra_params: Optional[dict[str, Any]] = None
    ) -> ChatCompletionResponse:
        if len(messages) == 0:
            raise ValueError("No messages provided")
        if messages[0].role != ChatRole.SYSTEM and self.default_system:
            messages = [system(self.default_system)] + messages

        data = {
            "model": self.api_model_name,
            "messages": [
                {"role": message.role, "content": message.content}
                for message in messages
            ],
        }

        response = self._request("/chat/completions", data, extra_params)
        return ChatCompletionResponse(**response)

    def embedding(
        self, input_text: str, extra_params: Optional[dict[str, Any]]
    ) -> list[float]:
        if len(input_text) == 0:
            raise ValueError("No input provided")

        data = {"model": self.api_model_name, "input": input_text}

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
        if self.extra_params is not None:
            data.update(self.extra_params)
        if extra_params is not None:
            data.update(extra_params)

        rate_limit_retry = 0
        rate_limit_backoff = 1.0

        response = None
        while rate_limit_retry < MAX_TOO_MANY_REQUESTS_RETRIES:
            url = f"{self.base_url}{url_part}"
            headers = {
                "Authorization": f"Bearer {self._get_api_key()}",
                "Content-Type": "application/json",
            }
            logger.info(f"Sending request to OpenAI: {url}, {data}")

            response = requests.post(
                url,
                timeout=self.timeout,
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
            if response
            else "Request failed"
        )

    def _get_api_key(self) -> str:
        return random.choice(self.api_keys)
