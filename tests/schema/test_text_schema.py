from __future__ import annotations

from blossom.schema import TextSchema


def test_text_schema_type_and_content() -> None:
    item = TextSchema(content="hello")
    assert item.type == "text"
    assert item.content == "hello"
