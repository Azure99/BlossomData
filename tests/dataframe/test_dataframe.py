from __future__ import annotations

import math

import pytest

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.schema.row_schema import RowSchema


def _make_df(values: list[int]) -> LocalDataFrame:
    return LocalDataFrame([RowSchema(data={"value": value}) for value in values])


def test_dataframe_metadata_and_aggregates() -> None:
    df = _make_df([1, 2, 3])
    expected_sum = 6
    expected_count = 3
    expected_max = 3

    updated = df.add_metadata(lambda s: {"double": s.data["value"] * 2})
    assert [row.metadata["double"] for row in updated.collect()] == [2, 4, 6]

    dropped = updated.drop_metadata(["double"])
    assert all("double" not in row.metadata for row in dropped.collect())

    assert df.sum(lambda s: s.data["value"]) == expected_sum
    assert df.mean(lambda s: s.data["value"]) == pytest.approx(2.0)
    assert df.count() == expected_count
    assert df.min(lambda s: s.data["value"]) == 1
    assert df.max(lambda s: s.data["value"]) == expected_max
    assert df.variance(lambda s: s.data["value"]) == pytest.approx(1.0)
    assert df.stddev(lambda s: s.data["value"]) == pytest.approx(1.0)
    assert set(df.unique(lambda s: s.data["value"] % 2)) == {0, 1}


def _grouped_values(
    dataframe: LocalDataFrame, key: str, field: str
) -> dict[int, float]:
    rows = dataframe.collect()
    return {int(row.data[key]): float(row.data[field]) for row in rows}


def test_grouped_dataframe_wrappers() -> None:
    df = _make_df([1, 2, 3, 4])
    grouped = df.group_by(lambda s: s.data["value"] % 2, name="parity")

    sums = _grouped_values(grouped.sum(lambda s: s.data["value"]), "parity", "sum")
    assert sums == {1: 4.0, 0: 6.0}

    means = _grouped_values(grouped.mean(lambda s: s.data["value"]), "parity", "mean")
    assert means[1] == pytest.approx(2.0)
    assert means[0] == pytest.approx(3.0)

    counts = _grouped_values(grouped.count(), "parity", "count")
    assert counts == {1: 2.0, 0: 2.0}

    mins = _grouped_values(grouped.min(lambda s: s.data["value"]), "parity", "min")
    assert mins == {1: 1.0, 0: 2.0}

    maxs = _grouped_values(grouped.max(lambda s: s.data["value"]), "parity", "max")
    assert maxs == {1: 3.0, 0: 4.0}

    variances = _grouped_values(
        grouped.variance(lambda s: s.data["value"]), "parity", "variance"
    )
    assert variances[1] == pytest.approx(2.0)
    assert variances[0] == pytest.approx(2.0)

    stddevs = _grouped_values(
        grouped.stddev(lambda s: s.data["value"]), "parity", "stddev"
    )
    expected_stddev = math.sqrt(2)
    assert stddevs[1] == pytest.approx(expected_stddev)
    assert stddevs[0] == pytest.approx(expected_stddev)

    unique_rows = grouped.unique(lambda s: s.data["value"]).collect()
    unique_values = {
        int(row.data["parity"]): set(row.data["unique"]) for row in unique_rows
    }
    assert unique_values == {1: {1, 3}, 0: {2, 4}}
