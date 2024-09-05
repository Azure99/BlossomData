from enum import Enum
from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatInvalidFilter(FilterOperator):
    class Rule(Enum):
        EMPTY_MESSAGES = 0
        EMPTY_CONTENT = 1
        INVALID_ROLE_ORDER = 2

    def __init__(
        self,
        rule: list[Rule] = [
            Rule.EMPTY_MESSAGES,
            Rule.EMPTY_CONTENT,
            Rule.INVALID_ROLE_ORDER,
        ],
    ):
        super().__init__()
        self.rule = rule

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_chat(item)
        if self.Rule.EMPTY_MESSAGES in self.rule:
            if len(_item.messages) == 0:
                return False

        if self.Rule.EMPTY_CONTENT in self.rule:
            for message in _item.messages:
                if not message.content:
                    return False

        if self.Rule.INVALID_ROLE_ORDER in self.rule:
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
