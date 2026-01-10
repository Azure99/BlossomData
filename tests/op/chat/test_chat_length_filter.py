from __future__ import annotations

from blossom.op.chat.chat_length_filter import ChatLengthFilter
from blossom.schema.chat_schema import ChatSchema, assistant, user


def test_chat_length_filter() -> None:
    chat = ChatSchema(messages=[user("12345"), assistant("67890")])
    op = ChatLengthFilter(user_max_len=4)
    assert op.process_item(chat) is False

    op_total = ChatLengthFilter(total_max_len=6)
    assert op_total.process_item(chat) is False
