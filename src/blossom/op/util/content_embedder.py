from typing import Any, Optional

from blossom.provider.provider import Provider


class ContentEmbedder:
    def __init__(self, provider: Provider) -> None:
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
