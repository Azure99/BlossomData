from __future__ import annotations

import pytest

from blossom.conf.config import ModelConfig
from blossom.op.util.content_translator import ContentTranslator
from tests.stubs import StubProvider


def _model_config() -> ModelConfig:
    return ModelConfig(
        name="stub-model",
        provider="stub",
        api_model_name="stub-model",
        config={},
        extra_params=None,
    )


def test_content_translator_extracts_result() -> None:
    provider = StubProvider(
        _model_config(), chat_response='```json\n{"result": "你好"}\n```'
    )
    translator = ContentTranslator(provider)
    result = translator.translate(
        "hello", target_language="Chinese", instruction_only=False
    )
    assert result == "你好"


def test_content_translator_invalid_result_raises() -> None:
    provider = StubProvider(_model_config(), chat_response='{"result": 123}')
    translator = ContentTranslator(provider)
    with pytest.raises(ValueError, match="Failed to translate text"):
        translator.translate(
            "hello", target_language="Chinese", instruction_only=False, max_retry=1
        )
