from blossom.op.map_operator import MapOperator
from blossom.schema.chat_schema import (
    ChatMessageContentText,
    ChatRole,
)
from blossom.schema.schema import Schema
from blossom.util.type import StrEnum


class ChatReasoningContentMerger(MapOperator):
    class Strategy(StrEnum):
        FIRST_TURN = "first_turn"
        LAST_TURN = "last_turn"
        MULTI_TURN = "multi_turn"

    def __init__(
        self,
        prefix: str = "<think>\n",
        suffix: str = "\n</think>",
        clear_reasoning: bool = False,
        force_wrapper: bool = False,
        strategy: Strategy = Strategy.MULTI_TURN,
    ):
        super().__init__()
        self.prefix = prefix
        self.suffix = suffix
        self.clear_reasoning = clear_reasoning
        self.force_wrapper = force_wrapper
        self.strategy = strategy

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        messages = [msg for msg in _item.messages if msg.role == ChatRole.ASSISTANT]
        if self.strategy == self.Strategy.FIRST_TURN:
            messages = [messages[0]]
        elif self.strategy == self.Strategy.LAST_TURN:
            messages = [messages[-1]]

        for message in messages:
            if not message.content:
                continue
            if not message.reasoning_content and not self.force_wrapper:
                continue

            reasoning_prefix = (
                self.prefix + (message.reasoning_content or "") + self.suffix
            )

            if isinstance(message.content, str):
                message.content = reasoning_prefix + message.content
            elif isinstance(message.content, list):
                for part in message.content:
                    if isinstance(part, ChatMessageContentText):
                        part.text = reasoning_prefix + part.text
                        break

        for message in _item.messages:
            if self.clear_reasoning:
                message.reasoning_content = None

        return self._cast_base(_item)
