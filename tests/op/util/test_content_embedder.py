from __future__ import annotations

import pytest

from blossom.conf.config import ModelConfig
from blossom.op.util.content_embedder import ContentEmbedder
from tests.stubs import StubProvider


def _model_config() -> ModelConfig:
    return ModelConfig(
        name="stub-model",
        provider="stub",
        api_model_name="stub-model",
        config={},
        extra_params=None,
    )


def test_content_embedder() -> None:
    provider = StubProvider(_model_config(), embedding=[0.1, 0.2])
    embedder = ContentEmbedder(provider)
    assert embedder.embedding("hello") == [0.1, 0.2]


def test_content_embedder_retries_and_raises() -> None:
    class FailingProvider(StubProvider):
        def embedding(self, input_text, extra_params=None):  # type: ignore[override]
            raise RuntimeError("boom")

    provider = FailingProvider(_model_config())
    embedder = ContentEmbedder(provider)
    with pytest.raises(ValueError, match="Failed to embedd text"):
        embedder.embedding("hello", max_retry=2)
