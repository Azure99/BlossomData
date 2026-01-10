from __future__ import annotations

from typing import Any

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.metric_filter_operator import MetricFilterOperator
from blossom.schema.schema import Schema
from blossom.schema.text_schema import TextSchema

MAX_METRIC_LENGTH = 3


class _LengthMetricFilter(MetricFilterOperator):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.calls = 0

    def compute_metrics(self, item: Schema) -> int:
        self.calls += 1
        _item = self._cast_text(item)
        return len(_item.content)

    def should_keep(self, item: Schema, metrics: int) -> bool:
        return metrics <= MAX_METRIC_LENGTH


class _DictLengthMetricFilter(MetricFilterOperator):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.calls = 0

    def compute_metrics(self, item: TextSchema) -> dict[str, int]:
        self.calls += 1
        return {"length": len(item.content)}

    def should_keep(self, item: TextSchema, metrics: dict[str, int]) -> bool:
        return metrics["length"] > 1


def test_metric_filter_operator_metrics_only() -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="bbbb")])
    op = _LengthMetricFilter(metrics_only=True)
    result = op.process(df).collect()
    expected_count = 2
    assert len(result) == expected_count
    for item in result:
        assert op.metrics_metadata_key in item.metadata


def test_metric_filter_operator_recompute_flag() -> None:
    item = TextSchema(content="a")
    op = _LengthMetricFilter()
    item.metadata[op.metrics_metadata_key] = 1

    df = LocalDataFrame([item])
    op.process(df).collect()
    assert op.calls == 0


def test_metric_filter_operator_metrics_flow() -> None:
    df = LocalDataFrame([TextSchema(content="a"), TextSchema(content="bb")])

    op = _DictLengthMetricFilter(metrics_only=True)
    result = op.process(df).collect()
    assert result[0].metadata[op.metrics_metadata_key]["length"] == 1
    expected_length = 2
    expected_calls = 2
    assert result[1].metadata[op.metrics_metadata_key]["length"] == expected_length
    assert op.calls == expected_calls

    op_filter = _DictLengthMetricFilter(parallel=2)
    filtered = op_filter.process(df).collect()
    assert [item.content for item in filtered] == ["bb"]

    op_reverse = _DictLengthMetricFilter(reverse=True)
    reversed_items = op_reverse.process(df).collect()
    assert [item.content for item in reversed_items] == ["a"]
