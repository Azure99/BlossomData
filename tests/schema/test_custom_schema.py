from __future__ import annotations

from blossom.schema import CustomSchema


def test_custom_schema_data() -> None:
    item = CustomSchema(data={"x": 1})
    assert item.type == "custom"
    assert item.data == {"x": 1}
