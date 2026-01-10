from __future__ import annotations

from blossom.op.chat.chat_context_round_trimmer import ChatContextRoundTrimmer
from blossom.schema.chat_schema import ChatRole, ChatSchema, assistant, user


def test_chat_context_round_trimmer() -> None:
    messages = [
        user("1111"),
        assistant("2222"),
        user("3333"),
        assistant("4444"),
        user("5555"),
        assistant("6666"),
    ]
    chat = ChatSchema(messages=messages)
    trimmer = ChatContextRoundTrimmer(max_context_len=8)
    trimmed = trimmer.process_item(chat)
    expected_length = 2
    assert len(trimmed.messages) == expected_length
    assert trimmed.messages[0].role == ChatRole.USER
    assert trimmed.messages[1].role == ChatRole.ASSISTANT
