from __future__ import annotations

import pytest

from blossom.dataframe.aggregate import (
    Count,
    Max,
    Mean,
    Min,
    StdDev,
    Sum,
    Unique,
    Variance,
)
from blossom.schema.row_schema import RowSchema


def _schemas(values: list[float]) -> list[RowSchema]:
    return [RowSchema(data={"value": v}) for v in values]


def _value(schema: RowSchema) -> float:
    return float(schema.data["value"])


def _run_agg(agg, items: list[RowSchema]):
    acc = agg.initial_value
    for item in items:
        acc = agg.accumulate(acc, item)
    return agg.finalize(acc)


def test_basic_aggregates() -> None:
    items = _schemas([1, 2, 3])
    expected_sum = 6
    expected_mean = 2.0
    expected_count = 3
    expected_min = 1
    expected_max = 3
    assert _run_agg(Sum(_value), items) == expected_sum
    assert _run_agg(Mean(_value), items) == expected_mean
    assert _run_agg(Count(), items) == expected_count
    assert _run_agg(Min(_value), items) == expected_min
    assert _run_agg(Max(_value), items) == expected_max


def test_variance_and_stddev() -> None:
    items = _schemas([1, 2, 3])
    assert _run_agg(Variance(_value), items) == pytest.approx(1.0)
    assert _run_agg(StdDev(_value), items) == pytest.approx(1.0)


def test_unique() -> None:
    items = _schemas([1, 1, 2])
    result = _run_agg(Unique(_value), items)
    assert set(result) == {1.0, 2.0}


def test_empty_or_insufficient_raises() -> None:
    with pytest.raises(ValueError):
        _run_agg(Mean(_value), [])

    with pytest.raises(ValueError):
        _run_agg(Min(_value), [])

    with pytest.raises(ValueError):
        _run_agg(Max(_value), [])

    with pytest.raises(ValueError):
        _run_agg(Variance(_value), _schemas([1]))

    with pytest.raises(ValueError):
        _run_agg(StdDev(_value), _schemas([1]))
