import json
import random
import time
from typing import Any, Optional

import requests

from blossom.conf import ModelConfig
from blossom.log import logger
from blossom.provider.protocol import ChatCompletionResponse, EmbeddingResponse
from blossom.provider.provider import Provider
from blossom.schema.chat_schema import ChatMessage, ChatRole, system

HTTP_SUCCESS = 200
HTTP_TOO_MANY_REQUESTS = 429
HTTP_INTERNAL_SERVER_ERROR = 500

DEFAULT_BASE_URL = "https://api.openai.com/v1"
DEFAULT_TIMEOUT = 600

MAX_TOO_MANY_REQUESTS_RETRIES = 12
TOO_MANY_REQUESTS_BACKOFF_FACTOR = 1.5
BACKOFF_RANDOM_FACTOR = 0.5


class OpenAI(Provider):
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
            messages = [system(self.default_system), *messages]

        data = {
            "model": self.api_model_name,
            "messages": [message.model_dump() for message in messages],
        }

        response = self._request("/chat/completions", data, extra_params)
        return ChatCompletionResponse(**response)

    def embedding(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> list[float]:
        response = self.embedding_with_details(input_text, extra_params)
        return response.data[0].embedding

    def embedding_with_details(
        self, input_text: str, extra_params: Optional[dict[str, Any]] = None
    ) -> EmbeddingResponse:
        if len(input_text) == 0:
            raise ValueError("No input provided")

        data = {"model": self.api_model_name, "input": input_text}

        response = self._request("/embeddings", data, extra_params)
        return EmbeddingResponse(**response)

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
                data=json.dumps(data, ensure_ascii=False),
            )
            logger.info(f"OpenAI response: {response.text}")

            if response.status_code == HTTP_SUCCESS:
                return response.json()

            if (
                response.status_code == HTTP_TOO_MANY_REQUESTS
                or response.status_code >= HTTP_INTERNAL_SERVER_ERROR
            ):
                self._sleep_for_rate_limit(rate_limit_retry)
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

    @staticmethod
    def _sleep_for_rate_limit(retry_count: int) -> None:
        rate_limit_backoff = TOO_MANY_REQUESTS_BACKOFF_FACTOR**retry_count
        random_factor = random.uniform(-BACKOFF_RANDOM_FACTOR, BACKOFF_RANDOM_FACTOR)
        backoff_time = rate_limit_backoff * (1 + random_factor)
        logger.warning(f"Rate limit exceeded, retrying in {backoff_time:.2f} seconds")
        time.sleep(backoff_time)
