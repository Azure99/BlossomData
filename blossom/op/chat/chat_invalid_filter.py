from typing import Optional

from blossom.op.filter_operator import FilterOperator
from blossom.schema.chat_schema import ChatRole
from blossom.schema.schema import Schema
from blossom.util.type import StrEnum


class ChatInvalidFilter(FilterOperator):
    class Rule(StrEnum):
        EMPTY_MESSAGES = "empty_messages"
        EMPTY_CONTENT = "empty_content"
        INVALID_ROLE_ORDER = "invalid_role_order"

    def __init__(
        self,
        rules: Optional[list[Rule]] = None,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.rules = rules or [
            self.Rule.EMPTY_MESSAGES,
            self.Rule.EMPTY_CONTENT,
            self.Rule.INVALID_ROLE_ORDER,
        ]

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_chat(item)
        if self.Rule.EMPTY_MESSAGES in self.rules:
            if len(_item.messages) == 0:
                return False

        if self.Rule.EMPTY_CONTENT in self.rules:
            for message in _item.messages:
                if not message.content:
                    return False

        if self.Rule.INVALID_ROLE_ORDER in self.rules:
            messages = list(filter(lambda x: x.role != ChatRole.SYSTEM, _item.messages))
            expected_role = ChatRole.USER
            for message in messages:
                if message.role != expected_role:
                    return False

                if expected_role == ChatRole.USER:
                    expected_role = ChatRole.ASSISTANT
                else:
                    expected_role = ChatRole.USER

            if len(messages) % 2 != 0:
                return False

        return True
