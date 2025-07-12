from typing import Optional

from blossom.op.map_operator import MapOperator
from blossom.schema.chat_schema import ChatRole, ChatMessageContentText
from blossom.schema.schema import Schema
from blossom.util.text import replace_text


class ChatContentReplacer(MapOperator):
    def __init__(
        self,
        replacements: dict[str, str],
        roles: Optional[list[ChatRole]] = None,
        case_sensitive: bool = True,
        replace_reasoning: bool = False,
    ):
        super().__init__()
        self.replacements = replacements
        self.roles = roles or [ChatRole.ASSISTANT]
        self.case_sensitive = case_sensitive
        self.replace_reasoning = replace_reasoning

    def _replace_text(self, text: str) -> str:
        return replace_text(
            text=text,
            replacements=self.replacements,
            case_sensitive=self.case_sensitive,
        )

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)
        for message in _item.messages:
            if message.role in self.roles:
                if isinstance(message.content, str):
                    message.content = self._replace_text(message.content)
                elif isinstance(message.content, list):
                    for part in message.content:
                        if isinstance(part, ChatMessageContentText):
                            part.text = self._replace_text(part.text)
                
                if self.replace_reasoning and message.reasoning_content:
                    message.reasoning_content = self._replace_text(message.reasoning_content)
        return self._cast_base(item)
