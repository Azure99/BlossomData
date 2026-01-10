from __future__ import annotations

import pytest

from blossom.schema import SCHEMA_TYPE_TEXT, Schema, TextSchema


def test_schema_from_dict_registered_type() -> None:
    data = {"type": "text", "content": "hello", "failed": None, "metadata": None}
    schema = Schema.from_dict(data)
    assert isinstance(schema, TextSchema)
    assert schema.failed is False
    assert schema.metadata == {}
    assert schema.type == SCHEMA_TYPE_TEXT


def test_schema_from_dict_unknown_type() -> None:
    with pytest.raises(ValueError, match="Unsupported schema type"):
        Schema.from_dict({"type": "unknown"})
