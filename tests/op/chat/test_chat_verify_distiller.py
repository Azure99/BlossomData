from __future__ import annotations


from blossom.op.chat.chat_verify_distiller import ChatVerifyDistiller
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
    content: str, finish_reason=ChatCompletionFinishReason.STOP
) -> ChatCompletionResponse:
    return ChatCompletionResponse(
        choices=[
            ChatCompletionChoice(
                index=0,
                message=ChatMessage(role=ChatRole.ASSISTANT, content=content),
                finish_reason=finish_reason,
            )
        ],
        usage=UsageInfo(prompt_tokens=1, total_tokens=3, completion_tokens=1),
    )


def _schema() -> ChatSchema:
    return ChatSchema(messages=[user("q"), assistant("ref")])


def test_chat_verify_distiller_mode_none() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("answer")])
    distiller = ChatVerifyDistiller(model="m", mode=ChatVerifyDistiller.Mode.NONE)
    distiller.init_context(ctx)

    item = _schema()
    result = distiller.process_item(item)
    assert result.messages[-1].content == "answer"


def test_chat_verify_distiller_mode_regex() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("answer is 42")])
    distiller = ChatVerifyDistiller(
        model="m", mode=ChatVerifyDistiller.Mode.REGEX, reference_field="ref"
    )
    distiller.init_context(ctx)

    item = _schema()
    item.metadata["ref"] = "42"
    result = distiller.process_item(item)
    assert result.messages[-1].content == "answer is 42"


def test_chat_verify_distiller_mode_llm() -> None:
    ctx = QueuedStubContext(
        detail_responses=[_response("model")],
        chat_responses=["review", '{"consistent": true}'],
    )
    distiller = ChatVerifyDistiller(
        model="m", mode=ChatVerifyDistiller.Mode.LLM, reference_field="ref"
    )
    distiller.init_context(ctx)

    item = _schema()
    item.metadata["ref"] = "42"
    result = distiller.process_item(item)
    assert result.messages[-1].content == "model"


def test_chat_verify_distiller_mode_function() -> None:
    ctx = QueuedStubContext(detail_responses=[_response("answer")])
    distiller = ChatVerifyDistiller(
        model="m",
        mode=ChatVerifyDistiller.Mode.FUNCTION,
        validation_function=lambda *_: True,
    )
    distiller.init_context(ctx)

    item = _schema()
    result = distiller.process_item(item)
    assert result.messages[-1].content == "answer"


def test_chat_verify_distiller_incomplete_response_marks_failed() -> None:
    ctx = QueuedStubContext(
        detail_responses=[
            _response("partial", finish_reason=ChatCompletionFinishReason.LENGTH)
        ]
    )
    distiller = ChatVerifyDistiller(
        model="m",
        mode=ChatVerifyDistiller.Mode.REGEX,
        reference_field="ref",
        max_retry=1,
    )
    distiller.init_context(ctx)

    item = _schema()
    item.metadata["ref"] = "42"
    result = distiller.process_item(item)
    assert result.failed is True


def test_chat_verify_distiller_first_message_content_default() -> None:
    schema = ChatSchema(messages=[])
    assert (
        ChatVerifyDistiller._first_message_content(schema.messages, ChatRole.USER) == ""
    )
