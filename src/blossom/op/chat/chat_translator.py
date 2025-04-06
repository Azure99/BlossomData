from typing import Any, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.op.util.content_translator import ContentTranslator
from blossom.schema.chat_schema import ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema


class ChatTranslator(MapOperator):
    def __init__(
        self,
        model: str,
        target_language: str = "Chinese",
        roles: Optional[list[ChatRole]] = None,
        instruction_only: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.target_language = target_language
        self.roles = roles or [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT]
        self.instruction_only = instruction_only
        self.max_retry = max_retry
        self.extra_params = extra_params

    def _translate(self, content: str) -> str:
        translator = ContentTranslator(self.context.get_model(self.model))
        return translator.translate(
            content=content,
            target_language=self.target_language,
            instruction_only=self.instruction_only,
            max_retry=self.max_retry,
            extra_params=self.extra_params,
        )

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        for message in _item.messages:
            if message.role in self.roles:
                try:
                    if isinstance(message.content, str):
                        message.content = self._translate(message.content)
                    elif isinstance(message.content, list):
                        for part in message.content:
                            if isinstance(part, ChatMessageContentText):
                                part.text = self._translate(part.text)
                except Exception as e:
                    _item.failed = True
                    logger.exception(
                        f"Failed to translate message: {message.content}, {e}"
                    )

        return self._cast_base(_item)
