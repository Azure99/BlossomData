from __future__ import annotations

from blossom.op.text.text_repetition_filter import TextRepetitionFilter
from blossom.schema.text_schema import TextSchema


def test_text_repetition_filter() -> None:
    op = TextRepetitionFilter(n=2, max_ratio=0.5)
    assert op.process_item(TextSchema(content="abababab")) is False
    assert op.process_item(TextSchema(content="abcdefg")) is True
