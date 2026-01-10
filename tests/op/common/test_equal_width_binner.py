from __future__ import annotations

from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.common.equal_width_binner import EqualWidthBinner
from blossom.schema.row_schema import RowSchema


def test_equal_width_binner_metadata() -> None:
    data = [RowSchema(data={"value": v}) for v in [0, 5, 9]]
    df = LocalDataFrame(data)
    op = EqualWidthBinner(func=lambda s: s.data["value"], num_bins=3)
    result = op.process(df).collect()
    for item in result:
        assert "bin" in item.metadata
        assert "bin_label" in item.metadata
        assert item.metadata["bin"] in {0, 1, 2}
        assert item.metadata["bin_label"] in {0.0, 3.0, 6.0}
