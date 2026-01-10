from __future__ import annotations

import json
from dataclasses import dataclass

import pytest

from blossom.conf.config import ModelConfig
from blossom.provider.openai import OpenAI
from blossom.schema.chat_schema import user


@dataclass
class _FakeResponse:
    status_code: int
    payload: dict
    text: str = ""

    def json(self):
        return self.payload


def _model_config(**config_override) -> ModelConfig:
    config = {"key": "test-key"}
    config.update(config_override)
    return ModelConfig(
        name="demo",
        provider="openai",
        api_model_name="demo-model",
        config=config,
        extra_params=None,
    )


def test_load_api_keys_validation() -> None:
    with pytest.raises(ValueError, match="No API key provided"):
        OpenAI._load_api_keys(
            ModelConfig(
                name="demo",
                provider="openai",
                api_model_name="demo-model",
                config={},
                extra_params=None,
            )
        )

    with pytest.raises(ValueError, match="API keys must be strings"):
        OpenAI._load_api_keys(_model_config(key=123))

    keys = OpenAI._load_api_keys(_model_config(key="a", keys=["b"]))
    assert keys == ["a", "b"]


def test_chat_completion_adds_default_system(monkeypatch) -> None:
    config = _model_config(default_system="sys")
    client = OpenAI(config)

    captured = {}

    def _fake_request(self, url_part, data, extra_params):
        captured["data"] = data
        return {
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 1, "total_tokens": 1, "completion_tokens": 1},
        }

    monkeypatch.setattr(OpenAI, "_request", _fake_request)

    messages = [user("hi")]
    response = client.chat_completion_with_details(messages)

    assert response.choices[0].message.content == "ok"
    sent_messages = captured["data"]["messages"]
    assert sent_messages[0]["role"] == "system"
    assert sent_messages[0]["content"] == "sys"
    assert sent_messages[1]["role"] == "user"


def test_chat_completion_requires_messages() -> None:
    client = OpenAI(_model_config())
    with pytest.raises(ValueError, match="No messages provided"):
        client.chat_completion_with_details([])


def test_embedding_requires_input() -> None:
    client = OpenAI(_model_config())
    with pytest.raises(ValueError, match="No input provided"):
        client.embedding_with_details("")


def test_request_retries_and_merges_extra_params(monkeypatch) -> None:
    client = OpenAI(_model_config())
    calls = {"count": 0}

    def _post(url, timeout, headers, data):
        calls["count"] += 1
        if calls["count"] == 1:
            return _FakeResponse(429, {}, text="rate limit")
        payload = {
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "ok"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 1, "total_tokens": 1, "completion_tokens": 1},
        }
        return _FakeResponse(200, payload, text=json.dumps(payload))

    monkeypatch.setattr("blossom.provider.openai.requests.post", _post)
    monkeypatch.setattr("blossom.provider.openai.time.sleep", lambda *_: None)
    monkeypatch.setattr("blossom.provider.openai.random.uniform", lambda *_: 0.0)

    result = client._request(
        "/chat/completions", {"model": "demo"}, {"temperature": 0.5}
    )
    assert result["choices"][0]["message"]["content"] == "ok"
    expected_calls = 2
    assert calls["count"] == expected_calls


def test_request_raises_on_non_retryable(monkeypatch) -> None:
    client = OpenAI(_model_config())

    def _post(url, timeout, headers, data):
        return _FakeResponse(400, {}, text="bad request")

    monkeypatch.setattr("blossom.provider.openai.requests.post", _post)

    with pytest.raises(ValueError, match="Request failed with status code 400"):
        client._request("/chat/completions", {"model": "demo"}, None)
