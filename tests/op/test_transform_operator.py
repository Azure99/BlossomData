from __future__ import annotations

import pytest

from blossom.conf.config import Config
from blossom.context.context import Context
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.transform_operator import TransformOperator, context_transform_operator
from blossom.schema import RowSchema
from blossom.schema.text_schema import TextSchema


def test_transform_operator() -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="b")])
    op = TransformOperator(transform_func=lambda items: list(reversed(items)))
    result = op.process(df).collect()
    assert [item.content for item in result] == ["b", "a"]


def test_transform_operator_paths(sample_row_schema) -> None:
    df = LocalDataFrame([sample_row_schema])
    transformer = TransformOperator(transform_func=lambda items: items)
    assert transformer.process(df).collect()[0] is sample_row_schema

    with pytest.raises(NotImplementedError):
        TransformOperator().process_items([sample_row_schema])

    @context_transform_operator()
    def _ctx_transform(ctx: Context, items: list[RowSchema]) -> list[RowSchema]:
        for item in items:
            item.metadata["ctx"] = isinstance(ctx, Context)
        return items

    op = _ctx_transform
    op.init_context(Context(config=Config(models=[])))
    assert op.process_items([sample_row_schema])[0].metadata["ctx"] is True
