from __future__ import annotations

from blossom.conf.config import ModelConfig
from tests.stubs import StubProvider


def test_provider_init_prefers_api_model_name() -> None:
    config = ModelConfig(
        name="demo",
        provider="stub",
        api_model_name="api-demo",
        config={"token": "secret"},
        extra_params={"temperature": 0.1},
    )
    provider = StubProvider(config)
    assert provider.api_model_name == "api-demo"
    assert provider.config == {"token": "secret"}
    assert provider.extra_params == {"temperature": 0.1}


def test_provider_init_falls_back_to_name() -> None:
    config = ModelConfig(
        name="demo",
        provider="stub",
        api_model_name=None,
        config={},
        extra_params=None,
    )
    provider = StubProvider(config)
    assert provider.api_model_name == "demo"
