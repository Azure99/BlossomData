from __future__ import annotations

from typing import Any

import pytest

from blossom.conf.config import Config
from blossom.context.context import Context
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.filter_operator import FilterOperator, context_filter_operator
from blossom.op.map_operator import MapOperator, context_map_operator
from blossom.op.metric_filter_operator import MetricFilterOperator
from blossom.op.operator import Operator
from blossom.op.transform_operator import TransformOperator, context_transform_operator
from blossom.schema import (
    RowSchema,
    TextSchema,
)


class DummyOperator(Operator):
    def process(self, dataframe):
        return dataframe


class LengthMetricFilter(MetricFilterOperator):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.calls = 0

    def compute_metrics(self, item: TextSchema) -> dict[str, int]:
        self.calls += 1
        return {"length": len(item.content)}

    def should_keep(self, item: TextSchema, metrics: dict[str, int]) -> bool:
        return metrics["length"] > 1


def test_operator_cast_helpers(
    sample_text_schema, sample_chat_schema, sample_custom_schema, sample_row_schema
) -> None:
    op = DummyOperator()

    assert op._cast_base(sample_text_schema) is sample_text_schema
    assert op._cast_base_list([sample_row_schema]) == [sample_row_schema]

    assert op._cast_chat(sample_chat_schema) is sample_chat_schema
    assert op._cast_chat_list([sample_chat_schema]) == [sample_chat_schema]

    assert op._cast_custom(sample_custom_schema) is sample_custom_schema
    assert op._cast_custom_list([sample_custom_schema]) == [sample_custom_schema]

    assert op._cast_text(sample_text_schema) is sample_text_schema
    assert op._cast_text_list([sample_text_schema]) == [sample_text_schema]

    with pytest.raises(AssertionError):
        op._cast_chat(sample_text_schema)


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


def test_context_filter_operator_injects_context(sample_text_schema) -> None:
    @context_filter_operator()
    def _with_ctx(ctx: Context, item: TextSchema) -> bool:
        return isinstance(ctx, Context) and item.content == "ok"

    op = _with_ctx
    op.init_context(Context(config=Config(models=[])))
    assert op.process_item(TextSchema(content="ok")) is True


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


def test_metric_filter_operator_metrics_flow() -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="bb")])

    op = LengthMetricFilter(metrics_only=True)
    result = op.process(df).collect()
    assert result[0].metadata[op.metrics_metadata_key]["length"] == 1
    expected_length = 2
    expected_calls = 2
    assert result[1].metadata[op.metrics_metadata_key]["length"] == expected_length
    assert op.calls == expected_calls

    op_filter = LengthMetricFilter(parallel=2)
    filtered = op_filter.process(df).collect()
    assert [item.content for item in filtered] == ["bb"]

    op_reverse = LengthMetricFilter(reverse=True)
    reversed_items = op_reverse.process(df).collect()
    assert [item.content for item in reversed_items] == ["a"]
