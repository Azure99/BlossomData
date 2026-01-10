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
