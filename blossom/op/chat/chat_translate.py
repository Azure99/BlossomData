from enum import Enum
from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_translator import TextTranslator
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatMessage, ChatRole


class ChatTranslate(MapOperator):
    class Role(Enum):
        ALL = 0
        SYSTEM = 1
        USER = 2
        ASSISTANT = 3
        SYSTEM_USER = 4

    def __init__(
        self,
        translate_model: str,
        target_language: str = "Chinese",
        role: Role = Role.ALL,
        instruction_only: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.translate_model = translate_model
        self.target_language = target_language
        self.role = role
        self.instruction_only = instruction_only
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        translator = TextTranslator(self.context.get_model(self.translate_model))
        for message in _item.messages:
            if self._check_translate_role(message):
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

    def _check_translate_role(self, message: ChatMessage) -> bool:
        if self.role == ChatTranslate.Role.ALL:
            return True
        elif self.role == ChatTranslate.Role.SYSTEM:
            return message.role == ChatRole.SYSTEM
        elif self.role == ChatTranslate.Role.USER:
            return message.role == ChatRole.USER
        elif self.role == ChatTranslate.Role.ASSISTANT:
            return message.role == ChatRole.ASSISTANT
        elif self.role == ChatTranslate.Role.SYSTEM_USER:
            return message.role in [ChatRole.SYSTEM, ChatRole.USER]
        else:
            return False
