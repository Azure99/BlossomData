from enum import Enum
from typing import Any, Optional

from blossom.op.map_operator import MapOperator
from blossom.op.util.text_embedder import TextEmbedder
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatRole


class ChatEmbedding(MapOperator):
    class Strategy(Enum):
        FIRST = 0
        LAST = 1
        FULL = 2

    def __init__(
        self,
        model: str,
        roles: list[ChatRole] = [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT],
        strategy: Strategy = Strategy.FIRST,
        metadata_field: str = "embedding",
        overwrite_field: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.roles = roles
        self.strategy = strategy
        self.metadata_field = metadata_field
        self.overwrite_field = overwrite_field
        self.max_retry = max_retry
        self.extra_params = extra_params

    def process_item(self, item: BaseSchema) -> BaseSchema:
        _item = self._cast_chat(item)

        if not self.overwrite_field and _item.metadata.get(self.metadata_field):
            return self._cast_base(_item)

        messages = list(filter(lambda x: x.role in self.roles, _item.messages))
        if self.strategy == ChatEmbedding.Strategy.FIRST:
            messages = [messages[0]]
        elif self.strategy == ChatEmbedding.Strategy.LAST:
            messages = [messages[-1]]

        embedder = TextEmbedder(self.context.get_model(self.model))
        embeddings = []
        for message in messages:
            try:
                content_embedding = embedder.embedding(
                    content=message.content,
                    max_retry=self.max_retry,
                    extra_params=self.extra_params,
                )
                embeddings.append(content_embedding)
            except Exception:
                pass
        _item.metadata[self.metadata_field] = embeddings

        return self._cast_base(_item)
