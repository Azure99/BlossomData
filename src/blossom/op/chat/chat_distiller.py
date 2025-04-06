from typing import Any, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.provider.protocol import ChatCompletionFinishReason
from blossom.schema.chat_schema import ChatMessage, ChatRole
from blossom.schema.schema import Schema
from blossom.util.type import StrEnum

METADATA_RESPONSE_TRUNCATED = "response_truncated"


class ChatDistiller(MapOperator):
    class Strategy(StrEnum):
        FIRST_TURN = "first_turn"
        LAST_TURN = "last_turn"
        MULTI_TURN = "multi_turn"

    def __init__(
        self,
        model: str,
        strategy: Strategy = Strategy.FIRST_TURN,
        max_total_tokens: Optional[int] = None,
        stop_on_truncate: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.strategy = strategy
        self.max_total_tokens = max_total_tokens
        self.stop_on_truncate = stop_on_truncate
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        new_messages: list[ChatMessage] = []
        for _ in range(self.max_retry):
            try:
                new_messages, truncated = self._process_item_messages(_item.messages)
                if truncated:
                    _item.metadata[METADATA_RESPONSE_TRUNCATED] = True
                break
            except Exception as e:
                logger.exception(f"Failed to distill chat: {e}")

        if new_messages:
            _item.messages = new_messages
        else:
            _item.failed = True

        return self._cast_base(_item)

    def _process_item_messages(
        self, messages: list[ChatMessage]
    ) -> tuple[list[ChatMessage], bool]:
        if self.strategy == ChatDistiller.Strategy.FIRST_TURN:
            return self._process_first_turn(messages)
        elif self.strategy == ChatDistiller.Strategy.LAST_TURN:
            return self._process_last_turn(messages)
        elif self.strategy == ChatDistiller.Strategy.MULTI_TURN:
            return self._process_multi_turn(messages)

        raise NotImplementedError("Distill strategy not implemented")

    def _process_first_turn(
        self, messages: list[ChatMessage]
    ) -> tuple[list[ChatMessage], bool]:
        messages = list(filter(lambda x: x.role != ChatRole.ASSISTANT, messages))
        for i, message in enumerate(messages):
            if message.role == ChatRole.USER:
                messages = messages[: i + 1]
                break

        new_messages, truncated, _ = self._add_assistant_reply(messages)
        return new_messages, truncated

    def _process_last_turn(
        self, messages: list[ChatMessage]
    ) -> tuple[list[ChatMessage], bool]:
        for i, message in enumerate(reversed(messages)):
            if message.role == ChatRole.ASSISTANT:
                cut_off_index = len(messages) - 1 - i
                messages = messages[:cut_off_index]
                break

        new_messages, truncated, _ = self._add_assistant_reply(messages)
        return new_messages, truncated

    def _process_multi_turn(
        self, messages: list[ChatMessage]
    ) -> tuple[list[ChatMessage], bool]:
        has_truncated = False
        messages = list(filter(lambda x: x.role != ChatRole.ASSISTANT, messages))

        new_messages = []
        for message in messages:
            new_messages.append(message)
            if message.role == ChatRole.USER:
                new_messages, truncated, total_tokens = self._add_assistant_reply(
                    new_messages
                )

                if truncated:
                    has_truncated = True
                    if self.stop_on_truncate:
                        break

                if self.max_total_tokens and total_tokens >= self.max_total_tokens:
                    break

        return new_messages, has_truncated

    def _add_assistant_reply(
        self, messages: list[ChatMessage]
    ) -> tuple[list[ChatMessage], bool, int]:
        response = self.context.chat_completion_with_details(
            model=self.model, messages=messages, extra_params=self.extra_params
        )
        choice = response.choices[0]

        new_messages = [*messages, choice.message]
        response_truncated = choice.finish_reason == ChatCompletionFinishReason.LENGTH
        total_tokens = response.usage.total_tokens

        return new_messages, response_truncated, total_tokens
