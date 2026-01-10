from __future__ import annotations


from blossom.op.chat.chat_distiller import ChatDistiller
from blossom.provider.protocol import (
    ChatCompletionChoice,
    ChatCompletionFinishReason,
    ChatCompletionResponse,
    UsageInfo,
)
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatRole,
    ChatSchema,
    assistant,
    user,
)
from tests.stubs import QueuedStubContext


def _response(
    content: str, finish_reason=ChatCompletionFinishReason.STOP, total_tokens: int = 1
) -> ChatCompletionResponse:
    return ChatCompletionResponse(
        choices=[
            ChatCompletionChoice(
                index=0,
                message=ChatMessage(role=ChatRole.ASSISTANT, content=content),
                finish_reason=finish_reason,
            )
        ],
        usage=UsageInfo(
            prompt_tokens=1, total_tokens=total_tokens, completion_tokens=1
        ),
    )


def _schema() -> ChatSchema:
    return ChatSchema(
        messages=[user("q1"), assistant("a1"), user("q2"), assistant("a2")]
    )


def test_chat_distiller_first_turn() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("reply")])
    distiller = ChatDistiller(model="m", strategy=ChatDistiller.Strategy.FIRST_TURN)
    distiller.init_context(ctx)

    result = distiller.process_item(_schema())
    expected_message_count = 2
    assert len(result.messages) == expected_message_count
    assert result.messages[-1].content == "reply"


def test_chat_distiller_last_turn() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("reply")])
    distiller = ChatDistiller(model="m", strategy=ChatDistiller.Strategy.LAST_TURN)
    distiller.init_context(ctx)

    result = distiller.process_item(_schema())
    assert result.messages[-1].content == "reply"


def test_chat_distiller_multi_turn_truncates() -> None:
    ctx = QueuedStubContext(
        detail_responses=[
            _response("reply", finish_reason=ChatCompletionFinishReason.LENGTH)
        ]
    )
    distiller = ChatDistiller(
        model="m",
        strategy=ChatDistiller.Strategy.MULTI_TURN,
        stop_on_truncate=True,
    )
    distiller.init_context(ctx)

    result = distiller.process_item(_schema())
    assert result.metadata.get("response_truncated") is True


def test_chat_distiller_invalid_strategy_marks_failed() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("reply")])
    distiller = ChatDistiller(model="m", max_retry=1)
    distiller.init_context(ctx)
    distiller.strategy = "invalid"  # type: ignore[assignment]

    item = _schema()
    result = distiller.process_item(item)
    assert result.failed is True
