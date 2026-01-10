from __future__ import annotations

from blossom.conf.config import ModelConfig
from blossom.op.text.text_translator import TextTranslator
from blossom.schema.text_schema import TextSchema
from tests.stubs import StubContext, StubProvider


def _model_config() -> ModelConfig:
    return ModelConfig(
        name="stub-model",
        provider="stub",
        api_model_name="stub-model",
        config={},
        extra_params=None,
    )


def test_text_translator_success_and_failure() -> None:
    provider = StubProvider(
        _model_config(), chat_response='```json\n{"result": "hola"}\n```'
    )
    ctx = StubContext(provider)
    item = TextSchema(content="hello")

    translator = TextTranslator(model="stub", target_language="Spanish")
    translator.init_context(ctx)
    result = translator.process_item(item)
    assert result.content == "hola"

    bad_provider = StubProvider(_model_config(), chat_response="invalid")
    bad_ctx = StubContext(bad_provider)
    bad_item = TextSchema(content="hello")
    translator.init_context(bad_ctx)
    failed = translator.process_item(bad_item)
    assert failed.failed is True
