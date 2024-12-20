from enum import Enum

from pydantic import BaseModel

from blossom.schema.base_schema import BaseSchema, SchemaType


class ChatRole(Enum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class ChatMessage(BaseModel):
    role: ChatRole
    content: str


class ChatSchema(BaseSchema):
    type: SchemaType = SchemaType.CHAT
    messages: list[ChatMessage]


def system(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.SYSTEM, content=content)


def user(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.USER, content=content)


def assistant(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.ASSISTANT, content=content)
