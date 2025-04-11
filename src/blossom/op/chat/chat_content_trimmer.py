from typing import Optional

from blossom.op.map_operator import MapOperator
from blossom.schema.chat_schema import ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema


class ChatContentTrimmer(MapOperator):
    def __init__(
        self,
        roles: Optional[list[ChatRole]] = None,
        strip_chars: Optional[str] = None,
        trim_reasoning: bool = True,
    ):
        super().__init__()
        self.roles = roles or [ChatRole.ASSISTANT]
        self.strip_chars = strip_chars
        self.trim_reasoning = trim_reasoning

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        for message in _item.messages:
            if message.role not in self.roles:
                continue

            if isinstance(message.content, str):
                message.content = message.content.strip(self.strip_chars)
            elif isinstance(message.content, list):
                for part in message.content:
                    if isinstance(part, ChatMessageContentText):
                        part.text = part.text.strip(self.strip_chars)

            if self.trim_reasoning and message.reasoning_content:
                message.reasoning_content = message.reasoning_content.strip(
                    self.strip_chars
                )

        return self._cast_base(_item)
