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
        embeddings_field: str = "embeddings",
        overwrite_field: bool = False,
        merge_messages: bool = False,
        message_separator: str = "\n\n",
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
        parallel: int = 1,
    ):
        super().__init__(parallel=parallel)
        self.model = model
        self.roles = roles or [ChatRole.SYSTEM, ChatRole.USER, ChatRole.ASSISTANT]
        self.strategy = strategy
        self.embeddings_field = embeddings_field
        self.overwrite_field = overwrite_field
        self.merge_messages = merge_messages
        self.message_separator = message_separator
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

        if not self.overwrite_field and _item.metadata.get(self.embeddings_field):
            return self._cast_base(_item)

        messages = list(filter(lambda x: x.role in self.roles, _item.messages))
        if self.strategy == ChatEmbedder.Strategy.FIRST:
            messages = [messages[0]]
        elif self.strategy == ChatEmbedder.Strategy.LAST:
            messages = [messages[-1]]

        contents = []
        for message in messages:
            content = ""
            if isinstance(message.content, str):
                content += message.content
            elif isinstance(message.content, list):
                for part in message.content:
                    if isinstance(part, ChatMessageContentText):
                        content += part.text
            contents.append(content)

        if self.merge_messages:
            contents = [self.message_separator.join(contents)]

        embeddings = []
        for content in contents:
            try:
                embeddings.append(self._embedding(content))
            except Exception as e:
                logger.exception(f"Failed to embed message: {content}, {e}")
                _item.mark_failed(str(e))
                return self._cast_base(_item)

        _item.metadata[self.embeddings_field] = embeddings
        return self._cast_base(_item)
