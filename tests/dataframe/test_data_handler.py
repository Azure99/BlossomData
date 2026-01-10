from __future__ import annotations

from blossom.dataframe.data_handler import DefaultDataHandler, DictDataHandler
from blossom.schema import RowSchema, TextSchema


def test_default_data_handler_with_type() -> None:
    handler = DefaultDataHandler()
    data = {"type": "text", "content": "hello"}
    schema = handler.from_dict(data)
    assert isinstance(schema, TextSchema)
    dumped = handler.to_dict(schema)
    assert dumped["type"] == "text"
    assert dumped["content"] == "hello"


def test_default_data_handler_without_type() -> None:
    handler = DefaultDataHandler()
    data = {"name": "alice", "score": 99}
    schema = handler.from_dict(data)
    assert isinstance(schema, RowSchema)
    assert schema.data == data


def test_dict_data_handler_preserve_metadata() -> None:
    schema = RowSchema(data={"a": 1})
    schema.metadata["m"] = 2

    handler = DictDataHandler(preserve_metadata=False)
    assert handler.to_dict(schema) == {"a": 1}

    handler_meta = DictDataHandler(preserve_metadata=True)
    assert handler_meta.to_dict(schema) == {"m": 2, "a": 1}
