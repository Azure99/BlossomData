from typing import Callable

from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatLengthFilter(FilterOperator):
    def __init__(
        self,
        len_func: Callable[[str], int] = len,
        system_max_len: int = 4096,
        user_max_len: int = 4096,
        assistant_max_len: int = 4096,
        total_max_len: int = 16384,
    ):
        super().__init__()
        self.len_func = len_func
        self.system_max_len = system_max_len
        self.user_max_len = user_max_len
        self.assistant_max_len = assistant_max_len
        self.total_max_len = total_max_len

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_chat(item)

        total_len = 0
        for message in _item.messages:
            content_len = self.len_func(message.content)

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
