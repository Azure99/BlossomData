from __future__ import annotations


from blossom.op.chat.chat_reasoning_consistency_filter import (
    ChatReasoningConsistencyFilter,
)
from blossom.schema.chat_schema import ChatMessage, ChatRole, ChatSchema
from tests.stubs import QueuedStubContext


def _schema() -> ChatSchema:
    return ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="q"),
            ChatMessage(role=ChatRole.ASSISTANT, content="a"),
        ]
    )


def test_reasoning_consistency_with_reference_field() -> None:
    ctx = QueuedStubContext(chat_responses=["review", '{"inconsistent": false}'])
    filt = ChatReasoningConsistencyFilter(review_model="review", reference_field="ref")
    filt.init_context(ctx)

    item = _schema()
    item.metadata["ref"] = "reference"
    assert filt.process_item(item) is True


def test_reasoning_consistency_with_reasoning_model() -> None:
    ctx = QueuedStubContext(
        chat_responses=[
            "reference",
            "review",
            '{"inconsistent": true}',
        ]
    )
    filt = ChatReasoningConsistencyFilter(
        review_model="review", reasoning_model="reason"
    )
    filt.init_context(ctx)

    item = _schema()
    assert filt.process_item(item) is False


def test_reasoning_consistency_marks_failed_on_error() -> None:
    ctx = QueuedStubContext(chat_responses=["reference", "review", "bad-json"])
    filt = ChatReasoningConsistencyFilter(
        review_model="review", reasoning_model="reason", max_retry=1
    )
    filt.init_context(ctx)

    item = _schema()
    keep = filt.process_item(item)
    assert keep is True
    assert item.failed is True
