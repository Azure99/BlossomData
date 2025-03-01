from typing import Optional

from pydantic import BaseModel
from blossom.schema.chat_schema import ChatMessage
from blossom.util.type import StrEnum


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


class ChatCompletionResponseChoice(BaseModel):
    index: int
    message: ChatMessage
    finish_reason: ChatCompletionFinishReason


class UsageInfo(BaseModel):
    prompt_tokens: int = 0
    total_tokens: int = 0
    completion_tokens: Optional[int] = 0


class ChatCompletionResponse(BaseModel):
    choices: list[ChatCompletionResponseChoice]
    usage: UsageInfo
