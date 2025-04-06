from typing import Any, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.op.util.content_embedder import ContentEmbedder
from blossom.schema.schema import Schema


class TextEmbedder(MapOperator):
    def __init__(
        self,
        model: str,
        embedding_field: str = "embedding",
        overwrite_field: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.embedding_field = embedding_field
        self.overwrite_field = overwrite_field
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_text(item)

        if not self.overwrite_field and _item.metadata.get(self.embedding_field):
            return self._cast_base(_item)

        embedder = ContentEmbedder(self.context.get_model(self.model))
        try:
            _item.metadata[self.embedding_field] = [
                embedder.embedding(
                    content=_item.content,
                    max_retry=self.max_retry,
                    extra_params=self.extra_params,
                )
            ]
        except Exception as e:
            _item.failed = True
            logger.exception(f"Failed to embed text: {e}")

        return self._cast_base(_item)
