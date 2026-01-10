from __future__ import annotations

from blossom.op.chat.chat_embedder import ChatEmbedder
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
)


class RecordingEmbedder(ChatEmbedder):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.calls: list[str] = []

    def _embedding(self, text: str) -> list[float]:  # type: ignore[override]
        self.calls.append(text)
        return [1.0]


class FailingEmbedder(ChatEmbedder):
    def _embedding(self, text: str) -> list[float]:  # type: ignore[override]
        raise RuntimeError("boom")


def _schema() -> ChatSchema:
    return ChatSchema(
        messages=[
            ChatMessage(role=ChatRole.USER, content="first"),
            ChatMessage(
                role=ChatRole.ASSISTANT,
                content=[ChatMessageContentText(text="second")],
            ),
        ]
    )


def test_chat_embedder_strategies_and_merge() -> None:
    item = _schema()

    embedder = RecordingEmbedder(model="m", strategy=ChatEmbedder.Strategy.FIRST)
    embedder.process_item(item)
    assert embedder.calls == ["first"]

    embedder = RecordingEmbedder(model="m", strategy=ChatEmbedder.Strategy.LAST)
    embedder.process_item(_schema())
    assert embedder.calls == ["second"]

    embedder = RecordingEmbedder(
        model="m",
        strategy=ChatEmbedder.Strategy.FULL,
        merge_messages=True,
        message_separator="\n\n",
    )
    embedder.process_item(_schema())
    assert embedder.calls == ["first\n\nsecond"]


def test_chat_embedder_overwrite_and_failure() -> None:
    item = _schema()
    item.metadata["embeddings"] = ["existing"]

    embedder = RecordingEmbedder(model="m", overwrite_field=False)
    result = embedder.process_item(item)
    assert result.metadata["embeddings"] == ["existing"]

    failing = FailingEmbedder(model="m")
    failed_item = failing.process_item(_schema())
    assert failed_item.failed is True
