from __future__ import annotations

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
