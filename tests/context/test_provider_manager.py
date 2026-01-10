from __future__ import annotations

import pytest

from blossom.conf.config import Config, ModelConfig
from blossom.context.provider_manager import ProviderManager
from blossom.provider.openai import OpenAI


def _config_with_provider(provider: str) -> Config:
    return Config(
        models=[
            ModelConfig(
                name="demo",
                provider=provider,
                api_model_name="demo-model",
                config={"key": "test-key"},
                extra_params=None,
            )
        ]
    )


def test_load_providers_includes_openai() -> None:
    providers = ProviderManager._load_providers()
    assert "openai" in providers
    assert providers["openai"] is OpenAI


def test_get_model_caches_instance() -> None:
    manager = ProviderManager(_config_with_provider("openai"))
    first = manager.get_model("demo")
    second = manager.get_model("demo")
    assert first is second


def test_get_model_missing_model_raises() -> None:
    manager = ProviderManager(_config_with_provider("openai"))
    with pytest.raises(ValueError, match="Model missing not found"):
        manager.get_model("missing")


def test_get_model_missing_provider_raises() -> None:
    manager = ProviderManager(_config_with_provider("missing"))
    with pytest.raises(ValueError, match="Provider missing not found"):
        manager.get_model("demo")
