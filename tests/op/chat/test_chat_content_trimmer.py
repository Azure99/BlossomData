from __future__ import annotations

from blossom.op.chat.chat_content_trimmer import ChatContentTrimmer
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
)
from blossom.schema.chat_schema import text_content


def test_chat_content_trimmer_trims_text_and_reasoning() -> None:
    chat = ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.ASSISTANT,
                content=[text_content("  hi  ")],
                reasoning_content="  r  ",
            )
        ]
    )
    trimmer = ChatContentTrimmer(trim_reasoning=True)
    trimmed = trimmer.process_item(chat)
    assert isinstance(trimmed.messages[0].content, list)
    assert isinstance(trimmed.messages[0].content[0], ChatMessageContentText)
    assert trimmed.messages[0].content[0].text == "hi"
    assert trimmed.messages[0].reasoning_content == "r"
