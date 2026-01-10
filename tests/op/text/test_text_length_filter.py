from __future__ import annotations

from blossom.op.text.text_length_filter import TextLengthFilter
from blossom.schema.text_schema import TextSchema


def test_text_length_filter() -> None:
    item = TextSchema(content="12345")
    op = TextLengthFilter(max_len=4)
    assert op.process_item(item) is False
