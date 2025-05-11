from typing import Any, Optional, Union

from PIL import Image
from pydantic import BaseModel, field_validator

from blossom.schema.schema import Schema
from blossom.util.image import encode_image_file_to_url, encode_image_to_url
from blossom.util.type import StrEnum

SCHEMA_TYPE_CHAT = "chat"


class ChatRole(StrEnum):
    """
    Enumeration of possible roles in a chat conversation.

    Attributes:
        USER: Represents a user message
        ASSISTANT: Represents an assistant (AI) message
        SYSTEM: Represents a system message (instructions/context)
    """

    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class ChatMessageContentType(StrEnum):
    """
    Enumeration of content types that can be included in chat messages.

    Attributes:
        TEXT: Text content
        IMAGE_URL: Image content referenced by URL
    """

    TEXT = "text"
    IMAGE_URL = "image_url"


class ChatMessageContent(BaseModel):
    """
    Base class for all chat message content types.
    """

    type: ChatMessageContentType


class ChatMessageContentImageDetail(StrEnum):
    """
    Enumeration of image detail levels for image content.

    Attributes:
        AUTO: Automatically determine the detail level
        LOW: Low detail level (faster, fewer tokens)
        HIGH: High detail level (slower, more tokens)
    """

    AUTO = "auto"
    LOW = "low"
    HIGH = "high"


class ChatMessageContentImageURL(BaseModel):
    """
    Model for image content referenced by URL.

    Attributes:
        url: URL of the image
        detail: Detail level for the image
    """

    url: str
    detail: Optional[ChatMessageContentImageDetail] = ChatMessageContentImageDetail.AUTO


class ChatMessageContentImage(ChatMessageContent):
    """
    Model for image content in a chat message.

    Attributes:
        type: Always IMAGE_URL
        image_url: Image URL and detail level
    """

    type: ChatMessageContentType = ChatMessageContentType.IMAGE_URL
    image_url: ChatMessageContentImageURL


class ChatMessageContentText(ChatMessageContent):
    """
    Model for text content in a chat message.

    Attributes:
        type: Always TEXT
        text: The text content
    """

    type: ChatMessageContentType = ChatMessageContentType.TEXT
    text: str


class ChatMessage(BaseModel):
    """
    Model for a chat message, which can be from a user, assistant, or system.

    Attributes:
        role: The role of the message sender (user, assistant, or system)
        content: The content of the message (text or list of content elements)
        reasoning_content: Optional reasoning content for the message
    """

    role: ChatRole
    content: Union[str, list[ChatMessageContent]]
    reasoning_content: Optional[str] = None

    @field_validator("content", mode="before")
    def messages_deserialization(cls, v: Any) -> Any:
        """
        Deserialize message content from JSON format to appropriate content objects.

        Args:
            v: The content value to deserialize

        Returns:
            Deserialized content objects or original value if not deserializable
        """
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
        """
        Convert the message to a dictionary representation.

        Returns:
            Dictionary representation of the message
        """
        data = super().model_dump(*args, **kwargs)
        if isinstance(self.content, list):
            data["content"] = [content.model_dump() for content in self.content]
        if self.reasoning_content is None:
            data.pop("reasoning_content")
        return data


class ChatSchema(Schema):
    """
    Schema for chat data, representing a conversation with multiple messages.

    Attributes:
        type: Always "chat"
        messages: List of chat messages in the conversation
    """

    type: str = SCHEMA_TYPE_CHAT
    messages: list[ChatMessage]

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Convert the chat schema to a dictionary representation.

        Returns:
            Dictionary representation of the chat schema
        """
        data = super().model_dump(*args, **kwargs)
        data["messages"] = [message.model_dump() for message in self.messages]
        return data

    def add_message(self, role: ChatRole, content: str) -> "ChatSchema":
        """
        Add a new message to the conversation.

        Args:
            role: Role of the message sender
            content: Content of the message

        Returns:
            Self for method chaining
        """
        self.messages.append(ChatMessage(role=role, content=content))
        return self

    def add_system(self, content: str) -> "ChatSchema":
        """
        Add a system message to the conversation.

        Args:
            content: Content of the system message

        Returns:
            Self for method chaining
        """
        return self.add_message(ChatRole.SYSTEM, content)

    def add_user(self, content: str) -> "ChatSchema":
        """
        Add a user message to the conversation.

        Args:
            content: Content of the user message

        Returns:
            Self for method chaining
        """
        return self.add_message(ChatRole.USER, content)

    def add_assistant(self, content: str) -> "ChatSchema":
        """
        Add an assistant message to the conversation.

        Args:
            content: Content of the assistant message

        Returns:
            Self for method chaining
        """
        return self.add_message(ChatRole.ASSISTANT, content)

    def clear_messages(self) -> "ChatSchema":
        """
        Clear all messages from the conversation.

        Returns:
            Self for method chaining
        """
        self.messages = []
        return self

    def remove_last_message(self) -> "ChatSchema":
        """
        Remove the last message from the conversation.

        Returns:
            Self for method chaining
        """
        if self.messages:
            self.messages.pop()
        return self

    def remove_last_system(self) -> "ChatSchema":
        """
        Remove the last system message from the conversation.

        Returns:
            Self for method chaining
        """
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.SYSTEM:
                self.messages.pop(i)
                break
        return self

    def remove_last_user(self) -> "ChatSchema":
        """
        Remove the last user message from the conversation.

        Returns:
            Self for method chaining
        """
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.USER:
                self.messages.pop(i)
                break
        return self

    def remove_last_assistant(self) -> "ChatSchema":
        """
        Remove the last assistant message from the conversation.

        Returns:
            Self for method chaining
        """
        for i in reversed(range(len(self.messages))):
            if self.messages[i].role == ChatRole.ASSISTANT:
                self.messages.pop(i)
                break
        return self

    def first_message(
        self, default: Optional[ChatMessage] = None
    ) -> Optional[ChatMessage]:
        """
        Get the first message in the conversation.

        Args:
            default: Default value to return if no messages exist

        Returns:
            First message or default value
        """
        if not self.messages:
            return default
        return self.messages[0]

    def first_system(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the first system message in the conversation.

        Args:
            default: Default value to return if no system messages exist

        Returns:
            Content of the first system message or default value
        """
        for message in self.messages:
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def first_user(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the first user message in the conversation.

        Args:
            default: Default value to return if no user messages exist

        Returns:
            Content of the first user message or default value
        """
        for message in self.messages:
            if message.role == ChatRole.USER:
                return message.content
        return default

    def first_assistant(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the first assistant message in the conversation.

        Args:
            default: Default value to return if no assistant messages exist

        Returns:
            Content of the first assistant message or default value
        """
        for message in self.messages:
            if message.role == ChatRole.ASSISTANT:
                return message.content
        return default

    def last_message(
        self, default: Optional[ChatMessage] = None
    ) -> Optional[ChatMessage]:
        """
        Get the last message in the conversation.

        Args:
            default: Default value to return if no messages exist

        Returns:
            Last message or default value
        """
        if not self.messages:
            return default
        return self.messages[-1]

    def last_system(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the last system message in the conversation.

        Args:
            default: Default value to return if no system messages exist

        Returns:
            Content of the last system message or default value
        """
        for message in reversed(self.messages):
            if message.role == ChatRole.SYSTEM:
                return message.content
        return default

    def last_user(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the last user message in the conversation.

        Args:
            default: Default value to return if no user messages exist

        Returns:
            Content of the last user message or default value
        """
        for message in reversed(self.messages):
            if message.role == ChatRole.USER:
                return message.content
        return default

    def last_assistant(
        self, default: Optional[Union[str, list[ChatMessageContent]]] = None
    ) -> Optional[Union[str, list[ChatMessageContent]]]:
        """
        Get the content of the last assistant message in the conversation.

        Args:
            default: Default value to return if no assistant messages exist

        Returns:
            Content of the last assistant message or default value
        """
        for message in reversed(self.messages):
            if message.role == ChatRole.ASSISTANT:
                return message.content
        return default


def system(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    """
    Create a system message.

    Args:
        content: Content of the system message

    Returns:
        A ChatMessage with system role
    """
    return ChatMessage(role=ChatRole.SYSTEM, content=content)


def user(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    """
    Create a user message.

    Args:
        content: Content of the user message

    Returns:
        A ChatMessage with user role
    """
    return ChatMessage(role=ChatRole.USER, content=content)


def assistant(content: Union[str, list[ChatMessageContent]]) -> ChatMessage:
    """
    Create an assistant message.

    Args:
        content: Content of the assistant message

    Returns:
        A ChatMessage with assistant role
    """
    return ChatMessage(role=ChatRole.ASSISTANT, content=content)


def text_content(text: str) -> ChatMessageContent:
    """
    Create text content for a chat message.

    Args:
        text: The text content

    Returns:
        A ChatMessageContentText object
    """
    return ChatMessageContentText(text=text)


def image_content(
    url: str,
    detail: Optional[
        ChatMessageContentImageDetail
    ] = ChatMessageContentImageDetail.AUTO,
) -> ChatMessageContent:
    """
    Create image content for a chat message from a URL.

    Args:
        url: URL of the image
        detail: Detail level for the image

    Returns:
        A ChatMessageContentImage object
    """
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
    """
    Create image content for a chat message from an image file.

    Args:
        path: Path to the image file
        detail: Detail level for the image
        target_size: Target size for the image (optional)
        fmt: Image format (default: JPEG)

    Returns:
        A ChatMessageContentImage object
    """
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
    """
    Create image content for a chat message from a PIL Image object.

    Args:
        img: PIL Image object
        detail: Detail level for the image
        target_size: Target size for the image (optional)
        fmt: Image format (default: JPEG)

    Returns:
        A ChatMessageContentImage object
    """
    return image_content(
        encode_image_to_url(img, target_size=target_size, fmt=fmt), detail
    )
