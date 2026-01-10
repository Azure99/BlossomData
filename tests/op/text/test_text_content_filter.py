from __future__ import annotations

from blossom.op.text.text_content_filter import TextContentFilter
from blossom.schema.text_schema import TextSchema


def test_text_content_filter_case_insensitive() -> None:
    item = TextSchema(content="Hello BAD")
    op = TextContentFilter(contents=["bad"], case_sensitive=False)
    assert op.process_item(item) is False
