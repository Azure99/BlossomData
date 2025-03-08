from .chat_schema import (
    SCHEMA_TYPE_CHAT,
    ChatSchema,
    ChatMessage,
    ChatMessageContentType,
    ChatMessageContent,
    ChatMessageContentImageDetail,
    ChatMessageContentImageURL,
    ChatMessageContentImage,
    ChatMessageContentText,
    ChatRole,
    system,
    user,
    assistant,
    text_content,
    image_content,
)
from .custom_schema import (
    SCHEMA_TYPE_CUSTOM,
    CustomSchema,
)
from .schema import Schema
from .text_schema import (
    SCHEMA_TYPE_TEXT,
    TextSchema,
)

__all__ = [
    "SCHEMA_TYPE_CHAT",
    "SCHEMA_TYPE_CUSTOM",
    "SCHEMA_TYPE_TEXT",
    "ChatMessage",
    "ChatMessageContent",
    "ChatMessageContentImage",
    "ChatMessageContentImageDetail",
    "ChatMessageContentImageURL",
    "ChatMessageContentText",
    "ChatMessageContentType",
    "ChatRole",
    "ChatSchema",
    "CustomSchema",
    "Schema",
    "TextSchema",
    "assistant",
    "image_content",
    "system",
    "text_content",
    "user",
]

Schema.register_schema(SCHEMA_TYPE_CHAT, ChatSchema)
Schema.register_schema(SCHEMA_TYPE_CUSTOM, CustomSchema)
Schema.register_schema(SCHEMA_TYPE_TEXT, TextSchema)
