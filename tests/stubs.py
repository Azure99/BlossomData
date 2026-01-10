from __future__ import annotations

from typing import Any

from blossom.conf.config import ModelConfig
from blossom.provider.protocol import (
    ChatCompletionChoice,
    ChatCompletionFinishReason,
    ChatCompletionResponse,
    EmbeddingData,
    EmbeddingResponse,
    UsageInfo,
)
from blossom.provider.provider import Provider
from blossom.schema.chat_schema import ChatMessage, ChatRole


class StubProvider(Provider):
    def __init__(
        self,
        model_config: ModelConfig,
        chat_response: str = "ok",
        embedding: list[float] | None = None,
    ) -> None:
        super().__init__(model_config)
        self._chat_response = chat_response
        self._embedding = embedding or [0.0, 1.0]
        self.last_messages: list[ChatMessage] | None = None
        self.last_embedding_input: str | None = None

    def chat_completion(
        self, messages: list[ChatMessage], extra_params: dict[str, Any] | None = None
    ) -> str:
        self.last_messages = messages
        return self._chat_response

    def chat_completion_with_details(
        self, messages: list[ChatMessage], extra_params: dict[str, Any] | None = None
    ) -> ChatCompletionResponse:
        self.last_messages = messages
        choice = ChatCompletionChoice(
            index=0,
            message=ChatMessage(role=ChatRole.ASSISTANT, content=self._chat_response),
            finish_reason=ChatCompletionFinishReason.STOP,
        )
        usage = UsageInfo(prompt_tokens=0, total_tokens=0, completion_tokens=0)
        return ChatCompletionResponse(choices=[choice], usage=usage)

    def embedding(
        self, input_text: str, extra_params: dict[str, Any] | None = None
    ) -> list[float]:
        self.last_embedding_input = input_text
        return list(self._embedding)

    def embedding_with_details(
        self, input_text: str, extra_params: dict[str, Any] | None = None
    ) -> EmbeddingResponse:
        self.last_embedding_input = input_text
        data = [EmbeddingData(index=0, embedding=list(self._embedding))]
        usage = UsageInfo(prompt_tokens=0, total_tokens=0, completion_tokens=0)
        return EmbeddingResponse(data=data, usage=usage)


class StubContext:
    def __init__(self, provider: StubProvider) -> None:
        self._provider = provider

    def get_model(self, model_name: str) -> StubProvider:
        return self._provider

    def chat_completion(self, *args, **kwargs):
        return self._provider.chat_completion(*args, **kwargs)

    def chat_completion_with_details(self, *args, **kwargs):
        return self._provider.chat_completion_with_details(*args, **kwargs)

    def embedding(self, *args, **kwargs):
        return self._provider.embedding(*args, **kwargs)

    def embedding_with_details(self, *args, **kwargs):
        return self._provider.embedding_with_details(*args, **kwargs)


class QueuedStubContext:
    def __init__(
        self,
        chat_responses: list[str] | None = None,
        detail_responses: list[ChatCompletionResponse] | None = None,
        embeddings: list[list[float]] | None = None,
    ) -> None:
        self._chat_responses = list(chat_responses or [])
        self._detail_responses = list(detail_responses or [])
        self._embeddings = list(embeddings or [])
        self.last_chat_messages: list[ChatMessage] | None = None
        self.last_detail_messages: list[ChatMessage] | None = None
        self.last_embedding_input: str | None = None

    def chat_completion(self, messages, *args, **kwargs) -> str:
        self.last_chat_messages = messages
        if not self._chat_responses:
            raise AssertionError("No queued chat responses available")
        return self._chat_responses.pop(0)

    def chat_completion_with_details(self, messages, *args, **kwargs):
        self.last_detail_messages = messages
        if not self._detail_responses:
            raise AssertionError("No queued detailed chat responses available")
        return self._detail_responses.pop(0)

    def embedding(self, input_text: str, *args, **kwargs) -> list[float]:
        self.last_embedding_input = input_text
        if not self._embeddings:
            raise AssertionError("No queued embeddings available")
        return self._embeddings.pop(0)

    def embedding_with_details(
        self, input_text: str, *args, **kwargs
    ) -> EmbeddingResponse:
        self.last_embedding_input = input_text
        if not self._embeddings:
            raise AssertionError("No queued embeddings available")
        data = [EmbeddingData(index=0, embedding=self._embeddings.pop(0))]
        usage = UsageInfo(prompt_tokens=0, total_tokens=0, completion_tokens=0)
        return EmbeddingResponse(data=data, usage=usage)
