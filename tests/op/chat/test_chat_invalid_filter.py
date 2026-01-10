from __future__ import annotations

from blossom.op.chat.chat_invalid_filter import ChatInvalidFilter
from blossom.schema.chat_schema import ChatSchema, assistant, user


def test_chat_invalid_filter_rules() -> None:
    valid = ChatSchema(messages=[user("u"), assistant("a")])
    invalid_empty = ChatSchema(messages=[])
    invalid_order = ChatSchema(messages=[user("u"), user("u2")])

    op = ChatInvalidFilter()
    assert op.process_item(valid) is True
    assert op.process_item(invalid_empty) is False

    op_order = ChatInvalidFilter(rules=[ChatInvalidFilter.Rule.INVALID_ROLE_ORDER])
    assert op_order.process_item(invalid_order) is False
