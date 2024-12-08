from enum import Enum
from typing import Any, Optional
from blossom.log import logger

from blossom.op.map_operator import MapOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatMessage, ChatRole


class ChatDistill(MapOperator):
    class Strategy(Enum):
        FIRST_TURN = 0
        LAST_TURN = 1
        MULTI_TURN = 2

    def __init__(
        self,
        model: str,
        strategy: Strategy = Strategy.FIRST_TURN,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.strategy = strategy
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        new_messages = []
        for _ in range(self.max_retry):
            try:
                new_messages = self._process_item_messages(_item.messages)
                break
            except Exception as e:
                logger.exception(f"Failed to distill chat: {e}")

        if new_messages:
            _item.messages = new_messages
        else:
            _item.failed = True

        return self._cast_base(_item)

    def _process_item_messages(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        if self.strategy == ChatDistill.Strategy.FIRST_TURN:
            return self._process_first_turn(messages)
        elif self.strategy == ChatDistill.Strategy.LAST_TURN:
            return self._process_last_turn(messages)
        elif self.strategy == ChatDistill.Strategy.MULTI_TURN:
            return self._process_multi_turn(messages)

        raise NotImplementedError("Distill strategy not implemented")

    def _process_first_turn(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        messages = list(filter(lambda x: x.role != ChatRole.ASSISTANT, messages))
        for i, message in enumerate(messages):
            if message.role == ChatRole.USER:
                messages = messages[: i + 1]
                break

        return self._add_assistant_reply(messages)

    def _process_last_turn(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        for i, message in enumerate(reversed(messages)):
            if message.role == ChatRole.ASSISTANT:
                cut_off_index = len(messages) - 1 - i
                messages = messages[:cut_off_index]
                break

        return self._add_assistant_reply(messages)

    def _process_multi_turn(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        messages = list(filter(lambda x: x.role != ChatRole.ASSISTANT, messages))

        new_messages = []
        for message in messages:
            new_messages.append(message)
            if message.role == ChatRole.USER:
                new_messages = self._add_assistant_reply(new_messages)

        return new_messages

    def _add_assistant_reply(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        return self.context.chat_completion_with_messages(
            model=self.model, messages=messages, extra_params=self.extra_params
        )
