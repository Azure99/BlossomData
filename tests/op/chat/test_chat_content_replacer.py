from __future__ import annotations

from blossom.op.chat.chat_content_replacer import ChatContentReplacer
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
)
from blossom.schema.chat_schema import text_content


def test_chat_content_replacer_replaces_text() -> None:
    chat = ChatSchema(
        messages=[
            ChatMessage(
                role=ChatRole.ASSISTANT,
                content=[text_content("hi")],
            )
        ]
    )
    replacer = ChatContentReplacer(replacements={"hi": "hello"})
    replaced = replacer.process_item(chat)
    assert isinstance(replaced.messages[0].content, list)
    assert isinstance(replaced.messages[0].content[0], ChatMessageContentText)
    assert replaced.messages[0].content[0].text == "hello"
