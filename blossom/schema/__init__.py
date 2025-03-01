from .base_schema import SchemaType, BaseSchema
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
    "BaseSchema",
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

BaseSchema.register_schema(SchemaType.BASE, ChatSchema)
BaseSchema.register_schema(SchemaType.CHAT, ChatSchema)
BaseSchema.register_schema(SchemaType.CUSTOM, CustomSchema)
BaseSchema.register_schema(SchemaType.TEXT, TextSchema)
