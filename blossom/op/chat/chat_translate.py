from typing import Any, Optional
from blossom.log import logger

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_translator import TextTranslator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatTranslate(MapOperator):
    def __init__(
        self,
        model: str,
        target_language: str = "Chinese",
        roles: list[ChatRole] = [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT],
        instruction_only: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.target_language = target_language
        self.roles = roles
        self.instruction_only = instruction_only
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        translator = TextTranslator(self.context.get_model(self.model))
        for message in _item.messages:
            if message.role in self.roles:
                try:
                    message.content = translator.translate(
                        content=message.content,
                        target_language=self.target_language,
                        instruction_only=self.instruction_only,
                        max_retry=self.max_retry,
                        extra_params=self.extra_params,
                    )
                except Exception as e:
                    _item.failed = True
                    logger.exception(
                        f"Failed to translate message: {message.content}, {e}"
                    )

        return self._cast_base(_item)
