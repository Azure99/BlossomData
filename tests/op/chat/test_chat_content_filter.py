from __future__ import annotations

from blossom.op.chat.chat_content_filter import ChatContentFilter
from blossom.schema.chat_schema import ChatSchema, assistant


def test_chat_content_filter_case_insensitive() -> None:
    chat = ChatSchema(messages=[assistant("This is BAD")])
    op = ChatContentFilter(contents=["bad"], case_sensitive=False)
    assert op.process_item(chat) is False
