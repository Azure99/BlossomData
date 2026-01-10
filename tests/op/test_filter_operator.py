from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.filter_operator import FilterOperator
from blossom.schema.text_schema import TextSchema


def test_filter_operator_keeps_failed_items() -> None:
    ok = TextSchema(content="a")
    failed = TextSchema(content="b")
    failed.mark_failed("boom")

    op = FilterOperator(filter_func=lambda s: False)
    df = LocalDataFrame([ok, failed])
    result = op.process(df).collect()
    assert len(result) == 1
    assert result[0].failed is True
