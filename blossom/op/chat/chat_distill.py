from enum import Enum
from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatMessage, ChatRole


class ChatDistill(MapOperator):
    class Mode(Enum):
        FIRST_TURN = 0
        LAST_TURN = 1
        MULTI_TURN = 2

    def __init__(
        self,
        teacher_model: str,
        mode: Mode = Mode.FIRST_TURN,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.teacher_model = teacher_model
        self.mode = mode
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        for _ in range(self.max_retry):
            try:
                _item.messages = self._process_item_messages(_item.messages)
            except Exception:
                pass
        _item.messages = []

        return self._cast_base(_item)

    def _process_item_messages(self, messages: list[ChatMessage]) -> list[ChatMessage]:
        if self.mode == ChatDistill.Mode.FIRST_TURN:
            return self._process_first_turn(messages)
        elif self.mode == ChatDistill.Mode.LAST_TURN:
            return self._process_last_turn(messages)
        elif self.mode == ChatDistill.Mode.MULTI_TURN:
            return self._process_multi_turn(messages)

        raise NotImplementedError("Distill mode not implemented")

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
        content = self._chat_completion(messages)
        messages.append(ChatMessage(role=ChatRole.ASSISTANT, content=content))
        return messages

    def _chat_completion(self, messages: list[ChatMessage]) -> str:
        return self.context.chat_completion(
            model=self.teacher_model, messages=messages, extra_params=self.extra_params
        )
