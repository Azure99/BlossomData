from __future__ import annotations

from blossom.op.chat.chat_translator import ChatTranslator
from blossom.schema.chat_schema import ChatSchema, user


def test_chat_translator_uses_context(dummy_context, stub_provider) -> None:
    stub_provider._chat_response = '```json\n{"result": "你好"}\n```'
    op = ChatTranslator(model="stub-model", target_language="Chinese")
    op.init_context(dummy_context)
    chat = ChatSchema(messages=[user("hello")])
    result = op.process_item(chat)
    assert result.messages[0].content == "你好"
