from __future__ import annotations

from blossom.op.text.text_trimmer import TextTrimmer
from blossom.schema.text_schema import TextSchema


def test_text_trimmer() -> None:
    item = TextSchema(content="  hi  ")
    trimmer = TextTrimmer()
    trimmed = trimmer.process_item(item)
    assert trimmed.content == "hi"
