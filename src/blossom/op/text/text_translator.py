from typing import Any, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.op.util.content_translator import ContentTranslator
from blossom.schema.schema import Schema


class TextTranslator(MapOperator):
    def __init__(
        self,
        model: str,
        target_language: str = "Chinese",
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.target_language = target_language
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_text(item)

        translator = ContentTranslator(self.context.get_model(self.model))
        try:
            _item.content = translator.translate(
                content=_item.content,
                target_language=self.target_language,
                instruction_only=False,
                max_retry=self.max_retry,
                extra_params=self.extra_params,
            )
        except Exception as e:
            logger.exception(f"Failed to translate text: {e}")
            _item.failed = True

        return self._cast_base(_item)
