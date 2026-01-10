from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.common.metadata_clearer import MetadataClearer
from blossom.schema.row_schema import RowSchema


def test_metadata_clearer() -> None:
    item = RowSchema(data={"a": 1})
    item.metadata["x"] = 1
    df = LocalDataFrame([item])
    cleared = MetadataClearer().process(df).collect()
    assert cleared[0].metadata == {}
