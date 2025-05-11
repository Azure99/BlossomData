from typing import Optional

from pydantic import BaseModel

from blossom.schema.chat_schema import ChatMessage
from blossom.util.type import StrEnum


class UsageInfo(BaseModel):
    prompt_tokens: int = 0
    total_tokens: int = 0
    completion_tokens: Optional[int] = 0


class ChatCompletionFinishReason(StrEnum):
    STOP = "stop"
    LENGTH = "length"
    CONTENT_FILTER = "content_filter"
    TOOL_CALLS = "tool_calls"
    FUNCTION_CALL = "function_call"
    UNKNOWN = "unknown"

    @classmethod
    def _missing_(cls, value: object) -> "ChatCompletionFinishReason":
        return cls.UNKNOWN


class ChatCompletionChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: ChatCompletionFinishReason


class ChatCompletionResponse(BaseModel):
    choices: list[ChatCompletionChoice]
    usage: UsageInfo


class EmbeddingData(BaseModel):
    index: int
    embedding: list[float]


class EmbeddingResponse(BaseModel):
    data: list[EmbeddingData]
    usage: UsageInfo
