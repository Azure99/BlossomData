from typing import Optional

from blossom.op.filter_operator import FilterOperator
from blossom.op.util.char_repetition_filter import CharRepetitionFilter
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatRepetitionFilter(FilterOperator):
    def __init__(
        self,
        roles: Optional[list[ChatRole]] = None,
        n: int = 10,
        min_ratio: float = 0.0,
        max_ratio: float = 0.5,
    ):
        super().__init__()
        self.roles = (
            roles
            if roles
            else [
                ChatRole.ASSISTANT,
            ]
        )
        self.char_repetition_filter = CharRepetitionFilter(
            n=n, min_ratio=min_ratio, max_ratio=max_ratio
        )

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                if not self.char_repetition_filter.filter(message.content):
                    return False
        return True