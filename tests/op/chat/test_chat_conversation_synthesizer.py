from __future__ import annotations

import pytest

from blossom.op.chat.chat_conversation_synthesizer import ChatMultiTurnSynthesizer
from blossom.provider.protocol import (
    ChatCompletionChoice,
    ChatCompletionFinishReason,
    ChatCompletionResponse,
    UsageInfo,
)
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentImage,
    ChatMessageContentImageURL,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
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
        usage=UsageInfo(prompt_tokens=1, total_tokens=5, completion_tokens=1),
    )


def test_synthesizer_success_truncates_and_marks_metadata() -> None:
    ctx = QueuedStubContext(
        chat_responses=['{"next_user_question": "next?"}'],
        detail_responses=[
            _response("answer", finish_reason=ChatCompletionFinishReason.LENGTH)
        ],
    )
    synth = ChatMultiTurnSynthesizer(model="m", max_rounds=2)
    synth.init_context(ctx)

    item = ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="hi"),
            ChatMessage(role=ChatRole.ASSISTANT, content="hello"),
            ChatMessage(role=ChatRole.ASSISTANT, content="extra"),
        ]
    )

    result = synth.process_item(item)
    assert result.metadata.get("response_truncated") is True
    assert result.messages[-1].content == "answer"


def test_synthesizer_invalid_json_marks_failed() -> None:
    ctx = QueuedStubContext(
        chat_responses=["not-json"], detail_responses=[_response("ok")]
    )
    synth = ChatMultiTurnSynthesizer(model="m", max_rounds=2, max_retry=1)
    synth.init_context(ctx)

    item = ChatSchema(messages=[ChatMessage(role=ChatRole.USER, content="hi")])
    result = synth.process_item(item)
    assert result.failed is True


def test_synthesizer_validation_errors() -> None:
    with pytest.raises(ValueError):
        ChatMultiTurnSynthesizer(model="m", input_truncate_rounds=0, max_rounds=1)

    with pytest.raises(ValueError):
        ChatMultiTurnSynthesizer(model="m", max_total_tokens=0, max_rounds=1)

    with pytest.raises(ValueError):
        ChatMultiTurnSynthesizer(model="m")

    with pytest.raises(ValueError):
        ChatMultiTurnSynthesizer(model="m", max_rounds_func="nope")

    with pytest.raises(ValueError):
        ChatMultiTurnSynthesizer(model="m", max_rounds=0)


def test_message_to_text_and_conversation_json() -> None:
    content = [
        ChatMessageContentText(text="hello"),
        ChatMessageContentImage(image_url=ChatMessageContentImageURL(url="http://x")),
    ]
    text = ChatMultiTurnSynthesizer._message_to_text(content)
    assert "hello" in text
    assert "non-text" in text

    messages = [
        ChatMessage(role=ChatRole.USER, content="hi"),
        ChatMessage(role=ChatRole.ASSISTANT, content=content),
    ]
    json_str = ChatMultiTurnSynthesizer._build_conversation_json(messages)
    assert "user" in json_str
    assert "assistant" in json_str


def test_resolve_max_rounds_func_validation() -> None:
    expected_max_rounds = 2
    synth = ChatMultiTurnSynthesizer(
        model="m", max_rounds_func=lambda _: expected_max_rounds
    )
    assert (
        synth._resolve_max_rounds(ChatSchema(messages=[user("hi")]))
        == expected_max_rounds
    )

    synth.max_rounds_func = lambda _: "bad"  # type: ignore[assignment]
    with pytest.raises(ValueError):
        synth._resolve_max_rounds(ChatSchema(messages=[user("hi")]))
