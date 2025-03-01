from .schema import SchemaType, Schema
from .chat_schema import (
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
from .custom_schema import CustomSchema
from .text_schema import TextSchema

__all__ = [
    "SchemaType",
    "Schema",
    "ChatSchema",
    "ChatMessage",
    "ChatMessageContentType",
    "ChatMessageContent",
    "ChatMessageContentImageDetail",
    "ChatMessageContentImageURL",
    "ChatMessageContentImage",
    "ChatMessageContentText",
    "ChatRole",
    "system",
    "user",
    "assistant",
    "text_content",
    "image_content",
    "CustomSchema",
    "TextSchema",
]

Schema.register_schema(SchemaType.BASE, ChatSchema)
Schema.register_schema(SchemaType.CHAT, ChatSchema)
Schema.register_schema(SchemaType.CUSTOM, CustomSchema)
Schema.register_schema(SchemaType.TEXT, TextSchema)
