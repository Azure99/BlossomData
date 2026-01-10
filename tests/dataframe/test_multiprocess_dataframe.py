from __future__ import annotations

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
