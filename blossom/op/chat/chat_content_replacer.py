from typing import Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text import replace_text
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatContentReplacer(MapOperator):
    def __init__(
        self,
        replacements: dict[str, str],
        roles: Optional[list[ChatRole]] = None,
        case_sensitive: bool = True,
    ):
        super().__init__()
        self.replacements = replacements
        self.roles = roles or [ChatRole.ASSISTANT]
        self.case_sensitive = case_sensitive

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                message.content = replace_text(
                    text=message.content,
                    replacements=self.replacements,
                    case_sensitive=self.case_sensitive,
                )
        return self._cast_base(item)
