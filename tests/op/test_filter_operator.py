from __future__ import annotations

import pytest

from blossom.conf.config import Config
from blossom.context.context import Context
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.filter_operator import FilterOperator, context_filter_operator
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


def test_filter_operator_parallel_reverse_and_skip_failed(sample_text_schema) -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="bb")])
    filt = FilterOperator(filter_func=lambda s: len(s.content) > 1, parallel=2)
    result = filt.process(df).collect()
    assert [item.content for item in result] == ["bb"]

    reverse = FilterOperator(filter_func=lambda s: len(s.content) > 1, reverse=True)
    reverse_result = reverse.process(df).collect()
    assert [item.content for item in reverse_result] == ["a"]

    failed_item = TextSchema(content="x")
    failed_item.mark_failed("boom")
    assert reverse.process_skip_failed(failed_item) is True

    with pytest.raises(NotImplementedError):
        FilterOperator().process_item(sample_text_schema)


def test_context_filter_operator_injects_context() -> None:
    @context_filter_operator()
    def _with_ctx(ctx: Context, item: TextSchema) -> bool:
        return isinstance(ctx, Context) and item.content == "ok"

    op = _with_ctx
    op.init_context(Context(config=Config(models=[])))
    assert op.process_item(TextSchema(content="ok")) is True
