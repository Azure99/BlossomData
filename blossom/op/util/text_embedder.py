from typing import Any, Optional

from blossom.provider.base_provider import BaseProvider


class TextEmbedder:
    def __init__(self, provider: BaseProvider) -> None:
        self.provider = provider

    def embedding(
        self,
        content: str,
        max_retry: int = 1,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> list[float]:

        last_exception = None
        for _ in range(max_retry):
            try:
                return self.provider.embedding(content, extra_params)
            except Exception as e:
                last_exception = e

        raise ValueError("Failed to embedd text") from last_exception