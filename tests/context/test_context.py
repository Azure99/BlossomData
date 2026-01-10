from __future__ import annotations


from blossom.conf.config import Config, ModelConfig
from blossom.context.context import Context
from blossom.schema.chat_schema import user
from tests.stubs import StubProvider


def _config() -> Config:
    return Config(
        models=[
            ModelConfig(
                name="stub",
                provider="stub",
                api_model_name="stub-model",
                config={},
                extra_params=None,
            )
        ]
    )


def test_context_accessors_and_delegation(monkeypatch) -> None:
    ctx = Context(config=_config())
    provider = StubProvider(_config().models[0], chat_response="pong", embedding=[1.0])

    monkeypatch.setattr(ctx.provider_manager, "get_model", lambda _: provider)

    assert ctx.get_config().models[0].name == "stub"
    assert ctx.get_provider_manager() is ctx.provider_manager

    messages = [user("ping")]
    assert ctx.chat_completion("stub", messages) == "pong"

    response = ctx.chat_completion_with_details("stub", messages)
    assert response.choices[0].message.content == "pong"

    assert ctx.embedding("stub", "hello") == [1.0]
    embedding_response = ctx.embedding_with_details("stub", "hello")
    assert embedding_response.data[0].embedding == [1.0]
