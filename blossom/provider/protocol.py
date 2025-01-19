from enum import Enum
from typing import Optional

from pydantic import BaseModel

from blossom.schema.chat_schema import ChatMessage


class ChatCompletionFinishReason(Enum):
    STOP = "stop"
    LENGTH = "length"
    CONTENT_FILTER = "content_filter"
    TOOL_CALLS = "tool_calls"
    FUNCTION_CALL = "function_call"


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
