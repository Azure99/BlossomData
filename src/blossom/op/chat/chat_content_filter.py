from typing import Optional

from blossom.op.filter_operator import FilterOperator
from blossom.schema.chat_schema import (
    ChatMessageContentText,
    ChatRole,
)
from blossom.schema.schema import Schema


class ChatContentFilter(FilterOperator):
    def __init__(
        self,
        contents: list[str],
        roles: Optional[list[ChatRole]] = None,
        case_sensitive: bool = True,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.contents = contents
        self.roles = roles or [ChatRole.ASSISTANT]
        self.case_sensitive = case_sensitive
        if not case_sensitive:
            self.contents = [content.lower() for content in contents]

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                text = ""
                if isinstance(message.content, str):
                    text = message.content
                elif isinstance(message.content, list):
                    for part in message.content:
                        if isinstance(part, ChatMessageContentText):
                            text += part.text
                if not self.case_sensitive:
                    text = text.lower()
                if any(content in text for content in self.contents):
                    return False
        return True
