from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.failed_item_filter import FailedItemFilter
from blossom.schema.text_schema import TextSchema


def test_failed_item_filter() -> None:
    ok = TextSchema(content="a")
    failed = TextSchema(content="b")
    failed.mark_failed("boom")

    df = LocalDataFrame([ok, failed])
    result = FailedItemFilter().process(df).collect()
    assert len(result) == 1
    assert result[0].content == "a"
