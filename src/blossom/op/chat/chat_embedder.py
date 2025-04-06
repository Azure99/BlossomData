from typing import Any, Optional

from blossom.log import logger
from blossom.op.map_operator import MapOperator
from blossom.op.util.content_embedder import ContentEmbedder
from blossom.schema.chat_schema import ChatMessageContentText, ChatRole
from blossom.schema.schema import Schema
from blossom.util.type import StrEnum


class ChatEmbedder(MapOperator):
    class Strategy(StrEnum):
        FIRST = "first"
        LAST = "last"
        FULL = "full"

    def __init__(
        self,
        model: str,
        roles: Optional[list[ChatRole]] = None,
        strategy: Strategy = Strategy.FIRST,
        embedding_field: str = "embedding",
        overwrite_field: bool = False,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.roles = roles or [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT]
        self.strategy = strategy
        self.embedding_field = embedding_field
        self.overwrite_field = overwrite_field
        self.max_retry = max_retry
        self.extra_params = extra_params

    def _embedding(self, text: str) -> list[float]:
        embedder = ContentEmbedder(self.context.get_model(self.model))
        return embedder.embedding(
            content=text,
            max_retry=self.max_retry,
            extra_params=self.extra_params,
        )

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_chat(item)

        if not self.overwrite_field and _item.metadata.get(self.embedding_field):
            return self._cast_base(_item)

        messages = list(filter(lambda x: x.role in self.roles, _item.messages))
        if self.strategy == ChatEmbedder.Strategy.FIRST:
            messages = [messages[0]]
        elif self.strategy == ChatEmbedder.Strategy.LAST:
            messages = [messages[-1]]

        embeddings = []
        for message in messages:
            try:
                content_embedding = []
                if isinstance(message.content, str):
                    content_embedding = [self._embedding(message.content)]
                elif isinstance(message.content, list):
                    for part in message.content:
                        if isinstance(part, ChatMessageContentText):
                            content_embedding.append(self._embedding(part.text))
                embeddings.extend(content_embedding)
            except Exception as e:
                _item.failed = True
                logger.exception(f"Failed to embed message: {message.content}, {e}")
                return self._cast_base(_item)
        _item.metadata[self.embedding_field] = embeddings

        return self._cast_base(_item)
