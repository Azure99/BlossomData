from __future__ import annotations

import pytest

from blossom.conf.config import Config
from blossom.context.context import Context
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.map_operator import MapOperator, context_map_operator
from blossom.schema import RowSchema
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


def test_map_operator_parallel_and_not_implemented(sample_row_schema) -> None:
    df = LocalDataFrame([sample_row_schema])
    mapper = MapOperator(map_func=lambda s: s, parallel=2)
    result = mapper.process(df).collect()
    assert result[0] is sample_row_schema

    with pytest.raises(NotImplementedError):
        MapOperator().process_item(sample_row_schema)


def test_context_map_operator_injects_context(sample_row_schema) -> None:
    @context_map_operator()
    def _with_ctx(ctx: Context, item: RowSchema) -> RowSchema:
        item.metadata["ctx"] = isinstance(ctx, Context)
        return item

    op = _with_ctx
    op.init_context(Context(config=Config(models=[])))
    result = op.process_item(sample_row_schema)
    assert result.metadata["ctx"] is True
