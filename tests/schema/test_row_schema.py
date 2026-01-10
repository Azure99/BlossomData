from __future__ import annotations

from blossom.schema import RowSchema


def test_row_schema_get_set_item() -> None:
    row = RowSchema(data={"a": 1})
    assert row["a"] == 1
    row["b"] = 2
    assert row.data == {"a": 1, "b": 2}
