from enum import Enum
from typing import Optional

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

    def add_message(self, role: ChatRole, content: str) -> "ChatSchema":
        self.messages.append(ChatMessage(role=role, content=content))
        return self

    def add_system(self, content: str) -> "ChatSchema":
        return self.add_message(ChatRole.SYSTEM, content)

    def add_user(self, content: str) -> "ChatSchema":
        return self.add_message(ChatRole.USER, content)

    def add_assistant(self, content: str) -> "ChatSchema":
        return self.add_message(ChatRole.ASSISTANT, content)

    def clear_messages(self) -> "ChatSchema":
        self.messages = []
        return self

    def remove_last_message(self) -> "ChatSchema":
        if self.messages:
            self.messages.pop()
        return self

    def remove_last_system(self) -> "ChatSchema":
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.SYSTEM:
                self.messages.pop(i)
                break
        return self

    def remove_last_user(self) -> "ChatSchema":
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.USER:
                self.messages.pop(i)
                break
        return self

    def remove_last_assistant(self) -> "ChatSchema":
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.ASSISTANT:
                self.messages.pop(i)
                break
        return self

    def first_message(
        self, default: Optional[ChatMessage] = None
    ) -> Optional[ChatMessage]:
        if not self.messages:
            return default
        return self.messages[0]

    def first_system(self, default: Optional[str] = None) -> Optional[str]:
        for message in self.messages:
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def first_user(self, default: Optional[str] = None) -> Optional[str]:
        for message in self.messages:
            if message.role == ChatRole.USER:
                return message.content
        return default

    def first_assistant(self, default: Optional[str] = None) -> Optional[str]:
        for message in self.messages:
            if message.role == ChatRole.ASSISTANT:
                return message.content
        return default

    def last_message(
        self, default: Optional[ChatMessage] = None
    ) -> Optional[ChatMessage]:
        if not self.messages:
            return default
        return self.messages[-1]

    def last_system(self, default: Optional[str] = None) -> Optional[str]:
        for message in reversed(self.messages):
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def last_user(self, default: Optional[str] = None) -> Optional[str]:
        for message in reversed(self.messages):
            if message.role == ChatRole.USER:
                return message.content
        return default

    def last_assistant(self, default: Optional[str] = None) -> Optional[str]:
        for message in reversed(self.messages):
            if message.role == ChatRole.ASSISTANT:
                return message.content
        return default


def system(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.SYSTEM, content=content)


def user(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.USER, content=content)


def assistant(content: str) -> ChatMessage:
    return ChatMessage(role=ChatRole.ASSISTANT, content=content)
