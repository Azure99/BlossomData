from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.transform_operator import TransformOperator
from blossom.schema.text_schema import TextSchema


def test_transform_operator() -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="b")])
    op = TransformOperator(transform_func=lambda items: list(reversed(items)))
    result = op.process(df).collect()
    assert [item.content for item in result] == ["b", "a"]
