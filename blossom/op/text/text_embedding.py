from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_embedder import TextEmbedder
from blossom.schema.base_schema import BaseSchema


class TextEmbedding(MapOperator):
    def __init__(
        self,
        model: str,
        metadata_field: str = "embedding",
        overwrite_field: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.metadata_field = metadata_field
        self.overwrite_field = overwrite_field
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_text(item)

        if not self.overwrite_field and _item.metadata.get(self.metadata_field):
            return self._cast_base(_item)

        embedder = TextEmbedder(self.context.get_model(self.model))
        try:
            _item.metadata[self.metadata_field] = [
                embedder.embedding(
                    content=_item.content,
                    max_retry=self.max_retry,
                    extra_params=self.extra_params,
                )
            ]
        except Exception:
            _item.content = ""

        return self._cast_base(_item)
