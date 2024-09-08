from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_translator import TextTranslator
from blossom.schema.base_schema import BaseSchema


class TextTranslate(MapOperator):
    def __init__(
        self,
        translate_model: str,
        target_language: str = "Chinese",
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.translate_model = translate_model
        self.target_language = target_language
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_text(item)

        translator = TextTranslator(self.context.get_model(self.translate_model))
        try:
            _item.content = translator.translate(
                content=_item.content,
                target_language=self.target_language,
                instruction_only=False,
                max_retry=self.max_retry,
                extra_params=self.extra_params,
            )
        except Exception:
            _item.content = ""

        return self._cast_base(_item)
