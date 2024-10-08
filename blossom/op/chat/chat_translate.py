from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_translator import TextTranslator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatTranslate(MapOperator):
    def __init__(
        self,
        translate_model: str,
        target_language: str = "Chinese",
        roles: Optional[list[ChatRole]] = None,
        instruction_only: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.translate_model = translate_model
        self.target_language = target_language
        self.roles = (
            roles if roles else [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT]
        )
        self.instruction_only = instruction_only
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        translator = TextTranslator(self.context.get_model(self.translate_model))
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
                except Exception:
                    message.content = ""

        return self._cast_base(_item)
