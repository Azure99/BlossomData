from typing import Optional

from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatContentFilter(FilterOperator):
    def __init__(
        self,
        contents: list[str],
        roles: Optional[list[ChatRole]] = None,
        case_sensitive: bool = True,
    ):
        super().__init__()
        self.contents = contents
        self.roles = roles or [ChatRole.ASSISTANT]
        self.case_sensitive = case_sensitive
        if not case_sensitive:
            self.contents = [content.lower() for content in contents]

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                text = message.content
                if not self.case_sensitive:
                    text = text.lower()
                if any(content in text for content in self.contents):
                    return False
        return True