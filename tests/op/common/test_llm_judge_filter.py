from __future__ import annotations

import pytest

from blossom.op.common.llm_judge_filter import (
    DEFAULT_PROMPT_TEMPLATE,
    LLMJudgeFilter,
    _chat_content_to_text,
)
from blossom.schema.chat_schema import (
    ChatMessage,
    ChatMessageContentImage,
    ChatMessageContentImageURL,
    ChatMessageContentText,
    ChatRole,
    ChatSchema,
)
from blossom.schema.text_schema import TextSchema
from tests.stubs import QueuedStubContext


def test_llm_judge_filter_init_validation() -> None:
    with pytest.raises(ValueError):
        LLMJudgeFilter(model="m", filter_guidance=" ")

    with pytest.raises(ValueError):
        LLMJudgeFilter(model="m")

    with pytest.raises(ValueError):
        LLMJudgeFilter(model="m", filter_guidance="ok", max_retry=0)


def test_llm_judge_filter_compute_metrics_success() -> None:
    ctx = QueuedStubContext(chat_responses=['{"analysis": "ok", "keep": true}'])
    filt = LLMJudgeFilter(model="m", filter_guidance="rule")
    filt.init_context(ctx)

    item = TextSchema(content="hello")
    metrics = filt.compute_metrics(item)
    assert metrics["keep"] is True
    assert metrics["analysis"] == "ok"
    assert metrics["raw"]


def test_llm_judge_filter_compute_metrics_invalid_keep_marks_failed() -> None:
    ctx = QueuedStubContext(chat_responses=['{"keep": "yes"}'])
    filt = LLMJudgeFilter(model="m", filter_guidance="rule", max_retry=1)
    filt.init_context(ctx)

    item = TextSchema(content="hello")
    metrics = filt.compute_metrics(item)
    assert item.failed is True
    assert metrics["keep"] is True


def test_llm_judge_filter_should_keep_invalid_metrics() -> None:
    filt = LLMJudgeFilter(model="m", filter_guidance="rule")
    item = TextSchema(content="hello")
    assert filt.should_keep(item, {"keep": "bad"}) is True
    assert item.failed is True


def test_llm_judge_filter_prompt_building_and_formatters() -> None:
    filt = LLMJudgeFilter(model="m", filter_guidance="rule")
    prompt = filt._build_prompt(TextSchema(content="hi"))
    assert "rule" in prompt
    assert "hi" in prompt
    assert DEFAULT_PROMPT_TEMPLATE.split("\n")[0] in prompt

    with pytest.raises(ValueError):
        bad = LLMJudgeFilter(model="m", filter_guidance_func=lambda _: " ")
        bad._build_prompt(TextSchema(content="hi"))

    with pytest.raises(ValueError):
        bad = LLMJudgeFilter(
            model="m",
            filter_guidance="rule",
            sample_format_func=lambda _: 123,
        )
        bad._build_prompt(TextSchema(content="hi"))


def test_chat_content_to_text_handles_images() -> None:
    content = [
        ChatMessageContentText(text="hello"),
        ChatMessageContentImage(image_url=ChatMessageContentImageURL(url="x")),
    ]
    text = _chat_content_to_text(content)
    assert "hello" in text
    assert "[image]" in text

    messages = [ChatMessage(role=ChatRole.USER, content=content)]
    chat = ChatSchema(messages=messages)
    filt = LLMJudgeFilter(model="m", filter_guidance="rule")
    prompt = filt._build_prompt(chat)
    assert "[image]" in prompt
