from typing import Callable

from blossom.op.map_operator import MapOperator
from blossom.schema.chat_schema import ChatMessage, ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema


class ChatContextRoundTrimmer(MapOperator):
    def __init__(
        self, max_context_len: int = 16384, len_func: Callable[[str], int] = len
    ):
        super().__init__()
        if max_context_len <= 0:
            raise ValueError("max_context_len must be greater than 0")
        self.max_context_len = max_context_len
        self.len_func = len_func

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        total_len = self._total_length(_item.messages)
        if total_len <= self.max_context_len:
            return self._cast_base(_item)

        messages = list(_item.messages)
        # Drop whole rounds from the tail until the total length fits.
        while total_len > self.max_context_len and messages:
            messages = self._drop_last_round(messages)
            total_len = self._total_length(messages)

        _item.messages = messages
        return self._cast_base(_item)

    def _total_length(self, messages: list[ChatMessage]) -> int:
        total_len = 0
        for message in messages:
            if isinstance(message.content, str):
                total_len += self.len_func(message.content)
            elif isinstance(message.content, list):
                for part in message.content:
                    if isinstance(part, ChatMessageContentText):
                        total_len += self.len_func(part.text)
        return total_len

    def _drop_last_round(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        # Drop the tail round so the last remaining message is an assistant turn.
        assistant_count = 0
        for idx in range(len(messages) - 1, -1, -1):
            if messages[idx].role == ChatRole.ASSISTANT:
                assistant_count += 1
                if assistant_count == 2:
                    return messages[: idx + 1]

        return []
