from __future__ import annotations

from blossom.provider.protocol import (
    ChatCompletionChoice,
    ChatCompletionFinishReason,
    ChatCompletionResponse,
    EmbeddingData,
    EmbeddingResponse,
    UsageInfo,
)
from blossom.schema.chat_schema import ChatMessage, ChatRole


def test_finish_reason_unknown_maps_to_unknown() -> None:
    reason = ChatCompletionFinishReason("nonsense")
    assert reason is ChatCompletionFinishReason.UNKNOWN


def test_protocol_models_round_trip() -> None:
    message = ChatMessage(role=ChatRole.ASSISTANT, content="ok")
    choice = ChatCompletionChoice(
        index=0,
        message=message,
        finish_reason="unexpected",
    )
    usage = UsageInfo()
    response = ChatCompletionResponse(choices=[choice], usage=usage)

    assert response.choices[0].finish_reason is ChatCompletionFinishReason.UNKNOWN
    assert response.usage.prompt_tokens == 0
    assert response.usage.total_tokens == 0
    assert response.usage.completion_tokens == 0


def test_embedding_response_model() -> None:
    embedding = EmbeddingResponse(
        data=[EmbeddingData(index=0, embedding=[0.1, 0.2])],
        usage=UsageInfo(prompt_tokens=1, total_tokens=1, completion_tokens=0),
    )
    assert embedding.data[0].embedding == [0.1, 0.2]
    assert embedding.usage.total_tokens == 1
