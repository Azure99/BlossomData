from __future__ import annotations

from blossom.op.chat.chat_reasoning_content_merger import ChatReasoningContentMerger
from blossom.schema.chat_schema import ChatSchema, assistant


def test_chat_reasoning_content_merger() -> None:
    chat = ChatSchema(messages=[assistant("answer")])
    chat.messages[0].reasoning_content = "reason"
    merger = ChatReasoningContentMerger(clear_reasoning=True)
    merged = merger.process_item(chat)
    assert merged.messages[0].content.startswith("<think>\nreason\n</think>")
    assert merged.messages[0].reasoning_content is None
