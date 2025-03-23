from typing import Callable

from blossom.op.filter_operator import FilterOperator
from blossom.schema.chat_schema import ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema


class ChatLengthFilter(FilterOperator):
    def __init__(
        self,
        len_func: Callable[[str], int] = len,
        system_max_len: int = 4096,
        user_max_len: int = 4096,
        assistant_max_len: int = 4096,
        total_max_len: int = 16384,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.len_func = len_func
        self.system_max_len = system_max_len
        self.user_max_len = user_max_len
        self.assistant_max_len = assistant_max_len
        self.total_max_len = total_max_len

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_chat(item)

        total_len = 0
        for message in _item.messages:
            content_len = 0
            if isinstance(message.content, str):
                content_len = self.len_func(message.content)
            elif isinstance(message.content, list):
                for part in message.content:
                    if isinstance(part, ChatMessageContentText):
                        content_len += self.len_func(part.text)

            total_len += content_len
            if total_len > self.total_max_len:
                return False

            if message.role == ChatRole.SYSTEM:
                if content_len > self.system_max_len:
                    return False
            elif message.role == ChatRole.USER:
                if content_len > self.user_max_len:
                    return False
            elif message.role == ChatRole.ASSISTANT:
                if content_len > self.assistant_max_len:
                    return False

        return True
