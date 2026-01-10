from __future__ import annotations

import pytest

from blossom.dataframe.multiprocess_dataframe import MultiProcessDataFrame
from blossom.schema.row_schema import RowSchema


def _inc(schema: RowSchema) -> RowSchema:
    schema.data["value"] += 1
    return schema


def _gt_one(schema: RowSchema) -> bool:
    return schema.data["value"] > 1


def test_multiprocess_dataframe_map_filter() -> None:
    data = [RowSchema(data={"value": v}) for v in [1, 2, 3]]
    df = MultiProcessDataFrame(data, num_workers=2)

    mapped = df.map(_inc).collect()
    assert [item.data["value"] for item in mapped] == [2, 3, 4]

    filtered = df.filter(_gt_one).collect()
    assert [item.data["value"] for item in filtered] == [2, 3]


def test_multiprocess_dataframe_parquet_io(tmp_path) -> None:
    pytest.importorskip("pyarrow")

    path = tmp_path / "data.parquet"
    df = MultiProcessDataFrame(
        [RowSchema(data={"a": 1}), RowSchema(data={"a": 2})],
        num_workers=2,
    )
    df.write_parquet(str(path))

    loaded = MultiProcessDataFrame(num_workers=2).read_parquet(str(path))
    values = sorted(row.data["a"] for row in loaded.collect())
    assert values == [1, 2]
