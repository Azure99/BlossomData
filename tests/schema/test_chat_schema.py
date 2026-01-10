from __future__ import annotations

from blossom.schema import (
    ChatMessage,
    ChatMessageContentImage,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
    assistant,
    system,
    user,
)


def test_chat_message_content_deserialization_and_dump() -> None:
    msg = ChatMessage(
        role=ChatRole.USER,
        content=[
            {"type": "text", "text": "hi"},
            {"type": "image_url", "image_url": {"url": "http://x", "detail": "low"}},
        ],
    )
    assert isinstance(msg.content, list)
    assert isinstance(msg.content[0], ChatMessageContentText)
    assert isinstance(msg.content[1], ChatMessageContentImage)

    dumped = msg.model_dump()
    assert dumped["content"][0]["type"] == "text"
    assert dumped["content"][1]["type"] == "image_url"
    assert "reasoning_content" not in dumped


def test_chat_schema_helpers() -> None:
    chat = ChatSchema(
        messages=[
            system("s1"),
            user("u1"),
            assistant("a1"),
            user("u2"),
            assistant("a2"),
        ]
    )
    assert chat.first_system() == "s1"
    assert chat.first_user() == "u1"
    assert chat.first_assistant() == "a1"
    assert chat.last_assistant() == "a2"

    chat.remove_last_assistant()
    assert chat.last_assistant() == "a1"

    chat.remove_last_user()
    assert chat.last_user() == "u1"

    chat.remove_last_system()
    assert chat.first_system() is None


def test_chat_schema_model_dump_messages() -> None:
    chat = ChatSchema(messages=[user("hello"), assistant("world")])
    dumped = chat.model_dump()
    assert isinstance(dumped["messages"], list)
    assert dumped["messages"][0]["role"] == "user"
    assert dumped["messages"][1]["role"] == "assistant"
