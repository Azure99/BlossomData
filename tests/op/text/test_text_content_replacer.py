from __future__ import annotations

from blossom.op.text.text_content_replacer import TextContentReplacer
from blossom.schema.text_schema import TextSchema


def test_text_content_replacer() -> None:
    item = TextSchema(content="hi")
    replacer = TextContentReplacer(replacements={"hi": "hello"})
    replaced = replacer.process_item(item)
    assert replaced.content == "hello"
