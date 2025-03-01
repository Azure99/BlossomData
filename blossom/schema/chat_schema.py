from typing import Any, Optional, Union

from PIL import Image
from pydantic import BaseModel, field_validator

from blossom.schema.schema import Schema, SchemaType
from blossom.util.image import encode_image_file_to_url, encode_image_to_url
from blossom.util.type import StrEnum


class ChatRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class ChatMessageContentType(StrEnum):
    TEXT = "text"
    IMAGE_URL = "image_url"


class ChatMessageContent(BaseModel):
    type: ChatMessageContentType


class ChatMessageContentImageDetail(StrEnum):
    AUTO = "auto"
    LOW = "low"
    HIGH = "high"


class ChatMessageContentImageURL(BaseModel):
    url: str
    detail: Optional[ChatMessageContentImageDetail] = ChatMessageContentImageDetail.AUTO


class ChatMessageContentImage(ChatMessageContent):
    type: ChatMessageContentType = ChatMessageContentType.IMAGE_URL
    image_url: ChatMessageContentImageURL


class ChatMessageContentText(ChatMessageContent):
    type: ChatMessageContentType = ChatMessageContentType.TEXT
    text: str


class ChatMessage(BaseModel):
    role: ChatRole
    content: Union[str, list[ChatMessageContent]]

    @field_validator("content", mode="before")
    def messages_deserialization(cls, v: Any) -> Any:
        if isinstance(v, list):
            if all(isinstance(item, dict) for item in v):
                content: list[ChatMessageContent] = []
                for item in v:
                    if item["type"] == ChatMessageContentType.TEXT.value:
                        content.append(ChatMessageContentText(**item))
                    elif item["type"] == ChatMessageContentType.IMAGE_URL.value:
                        content.append(ChatMessageContentImage(**item))
                return content
        return v

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        if isinstance(self.content, list):
            data["content"] = [content.model_dump() for content in self.content]
        return data


class ChatSchema(Schema):
    type: str = SchemaType.CHAT
    messages: list[ChatMessage]

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        data["messages"] = [message.model_dump() for message in self.messages]
        return data

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

    def first_system(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        for message in self.messages:
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def first_user(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        for message in self.messages:
            if message.role == ChatRole.USER:
                return message.content
        return default

    def first_assistant(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
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

    def last_system(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        for message in reversed(self.messages):
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def last_user(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        for message in reversed(self.messages):
            if message.role == ChatRole.USER:
                return message.content
        return default

    def last_assistant(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        for message in reversed(self.messages):
            if message.role == ChatRole.ASSISTANT:
                return message.content
        return default


def system(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    return ChatMessage(role=ChatRole.SYSTEM, content=content)


def user(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    return ChatMessage(role=ChatRole.USER, content=content)


def assistant(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    return ChatMessage(role=ChatRole.ASSISTANT, content=content)


def text_content(text: str) -> ChatMessageContent:
    return ChatMessageContentText(text=text)


def image_content(
    url: str,
    detail: Optional[
        ChatMessageContentImageDetail
    ] = ChatMessageContentImageDetail.AUTO,
) -> ChatMessageContent:
    return ChatMessageContentImage(
        image_url=ChatMessageContentImageURL(url=url, detail=detail)
    )


def image_content_from_file(
    path: str,
    detail: Optional[
        ChatMessageContentImageDetail
    ] = ChatMessageContentImageDetail.AUTO,
    target_size: Optional[int] = None,
    fmt: str = "JPEG",
) -> ChatMessageContent:
    return image_content(
        encode_image_file_to_url(path, target_size=target_size, fmt=fmt), detail
    )


def image_content_from_image(
    img: Image.Image,
    detail: Optional[
        ChatMessageContentImageDetail
    ] = ChatMessageContentImageDetail.AUTO,
    target_size: Optional[int] = None,
    fmt: str = "JPEG",
) -> ChatMessageContent:
    return image_content(
        encode_image_to_url(img, target_size=target_size, fmt=fmt), detail
    )
