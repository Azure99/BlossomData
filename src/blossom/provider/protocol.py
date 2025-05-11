from typing import Optional

from pydantic import BaseModel

from blossom.schema.chat_schema import ChatMessage
from blossom.util.type import StrEnum


class UsageInfo(BaseModel):
    """
    Information about token usage in a model API call.

    Attributes:
        prompt_tokens: Number of tokens in the prompt
        total_tokens: Total number of tokens used
        completion_tokens: Number of tokens in the completion (optional)
    """

    prompt_tokens: int = 0
    total_tokens: int = 0
    completion_tokens: Optional[int] = 0


class ChatCompletionFinishReason(StrEnum):
    """
    Enumeration of possible reasons why a chat completion finished.

    Attributes:
        STOP: The model reached a natural stopping point or stop token
        LENGTH: The response was cut off due to token limit
        CONTENT_FILTER: The response was filtered due to content policy
        TOOL_CALLS: The response was cut off after making tool calls
        FUNCTION_CALL: The response was cut off after making a function call
        UNKNOWN: The finish reason is unknown or not provided
    """

    STOP = "stop"
    LENGTH = "length"
    CONTENT_FILTER = "content_filter"
    TOOL_CALLS = "tool_calls"
    FUNCTION_CALL = "function_call"
    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> "ChatCompletionFinishReason":
        """
        Handle unknown finish reasons by returning UNKNOWN.

        Args:
            value: The value that wasn't found in the enum

        Returns:
            The UNKNOWN enum value
        """
        return cls.UNKNOWN


class ChatCompletionChoice(BaseModel):
    """
    A single completion choice from a chat completion response.

    Attributes:
        index: Index of this choice in the list of choices
        message: The message content of this choice
        finish_reason: Reason why the completion finished
    """

    index: int
    message: ChatMessage
    finish_reason: ChatCompletionFinishReason


class ChatCompletionResponse(BaseModel):
    """
    Response from a chat completion API call.

    Attributes:
        choices: List of completion choices
        usage: Token usage information
    """

    choices: list[ChatCompletionChoice]
    usage: UsageInfo


class EmbeddingData(BaseModel):
    """
    A single embedding vector with its index.

    Attributes:
        index: Index of this embedding in the list of embeddings
        embedding: Vector representation as a list of floats
    """

    index: int
    embedding: list[float]


class EmbeddingResponse(BaseModel):
    """
    Response from an embedding API call.

    Attributes:
        data: List of embedding vectors
        usage: Token usage information
    """

    data: list[EmbeddingData]
    usage: UsageInfo
