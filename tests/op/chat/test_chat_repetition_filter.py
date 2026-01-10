from __future__ import annotations

from blossom.op.chat.chat_repetition_filter import ChatRepetitionFilter
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
)


def test_chat_repetition_filter_detects_repetition() -> None:
    messages = [
        ChatMessage(role=ChatRole.ASSISTANT, content="aaaaaa"),
    ]
    schema = ChatSchema(messages=messages)
    filt = ChatRepetitionFilter(n=2, max_ratio=0.1)
    assert filt.process_item(schema) is False


def test_chat_repetition_filter_reasoning_and_parts() -> None:
    messages = [
        ChatMessage(
            role=ChatRole.ASSISTANT,
            content=[ChatMessageContentText(text="clean text")],
            reasoning_content="aaaaaa",
        ),
    ]
    schema = ChatSchema(messages=messages)
    filt = ChatRepetitionFilter(n=2, max_ratio=0.1, filter_reasoning=True)
    assert filt.process_item(schema) is False

    filt_ok = ChatRepetitionFilter(n=2, max_ratio=1.0, filter_reasoning=False)
    assert filt_ok.process_item(schema) is True
