from typing import Optional

from blossom.op.filter_operator import FilterOperator
from blossom.op.util.char_repetition_filter import CharRepetitionFilter
from blossom.schema.chat_schema import ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema


class ChatRepetitionFilter(FilterOperator):
    def __init__(
        self,
        roles: Optional[list[ChatRole]] = None,
        n: int = 10,
        min_ratio: float = 0.0,
        max_ratio: float = 0.5,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.roles = roles or [ChatRole.ASSISTANT]
        self.char_repetition_filter = CharRepetitionFilter(
            n=n, min_ratio=min_ratio, max_ratio=max_ratio
        )

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                if isinstance(message.content, str):
                    if not self.char_repetition_filter.filter(message.content):
                        return False
                elif isinstance(message.content, list):
                    for part in message.content:
                        if isinstance(part, ChatMessageContentText):
                            if not self.char_repetition_filter.filter(part.text):
                                return False
        return True
