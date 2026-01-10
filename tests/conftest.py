from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pytest

from blossom.conf.config import ModelConfig
from blossom.schema import (
    ChatSchema,
    CustomSchema,
    RowSchema,
    TextSchema,
    assistant,
    user,
)
from tests.stubs import StubContext, StubProvider


@pytest.fixture
def model_config() -> ModelConfig:
    return ModelConfig(
        name="stub-model",
        provider="stub",
        api_model_name="stub-model",
        config={},
        extra_params=None,
    )


@pytest.fixture
def stub_provider(model_config: ModelConfig) -> StubProvider:
    return StubProvider(model_config)


@pytest.fixture
def dummy_context(stub_provider: StubProvider) -> StubContext:
    return StubContext(stub_provider)


@pytest.fixture
def sample_text_schema() -> TextSchema:
    return TextSchema(content="hello world")


@pytest.fixture
def sample_chat_schema() -> ChatSchema:
    return ChatSchema(messages=[user("hi"), assistant("hello")])


@pytest.fixture
def sample_row_schema() -> RowSchema:
    return RowSchema(data={"name": "alice", "score": 42})


@pytest.fixture
def sample_custom_schema() -> CustomSchema:
    return CustomSchema(data={"custom": True})


@pytest.fixture
def write_jsonl(tmp_path: Path) -> Callable[[list[dict[str, object]], str], Path]:
    def _write(rows: list[dict[str, object]], filename: str = "data.jsonl") -> Path:
        path = tmp_path / filename
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        return path

    return _write
