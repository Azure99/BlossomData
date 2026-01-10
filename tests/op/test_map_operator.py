from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.map_operator import MapOperator
from blossom.schema.text_schema import TextSchema


def test_map_operator_skips_failed() -> None:
    ok = TextSchema(content="a")
    failed = TextSchema(content="b")
    failed.mark_failed("boom")

    op = MapOperator(map_func=lambda s: TextSchema(content=s.content.upper()))
    df = LocalDataFrame([ok, failed])
    result = op.process(df).collect()
    assert result[0].content == "A"
    assert result[1].content == "b"
    assert result[1].failed is True
