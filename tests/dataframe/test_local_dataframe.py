from __future__ import annotations

import json

import pytest

from blossom.dataframe.aggregate import Sum
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.schema.row_schema import RowSchema


def _values(items: list[RowSchema]) -> list[int]:
    return [int(item.data["value"]) for item in items]


def test_local_dataframe_basic_ops() -> None:
    data = [RowSchema(data={"value": v}) for v in [1, 2, 3]]
    df = LocalDataFrame(data)

    mapped = df.map(lambda s: RowSchema(data={"value": s.data["value"] + 1}))
    assert _values(mapped.collect()) == [2, 3, 4]

    filtered = df.filter(lambda s: s.data["value"] > 1)
    assert _values(filtered.collect()) == [2, 3]

    transformed = df.transform(lambda items: list(reversed(items)))
    assert _values(transformed.collect()) == [3, 2, 1]

    sorted_df = df.sort(lambda s: s.data["value"], ascending=False)
    assert _values(sorted_df.collect()) == [3, 2, 1]

    limited = df.limit(2)
    assert _values(limited.collect()) == [1, 2]

    shuffled = df.shuffle().collect()
    assert sorted(_values(shuffled)) == [1, 2, 3]

    splits = df.split(2)
    assert [len(s.collect()) for s in splits] == [2, 1]

    unioned = df.union(LocalDataFrame([RowSchema(data={"value": 4})]))
    assert _values(unioned.collect()) == [1, 2, 3, 4]

    total = df.aggregate(Sum(lambda s: s.data["value"]))
    expected_total = 6
    assert total == expected_total

    grouped = df.group_by(lambda s: s.data["value"] % 2, name="parity").count()
    grouped_rows = grouped.collect()
    grouped_map = {row.data["parity"]: row.data["count"] for row in grouped_rows}
    assert grouped_map == {0: 1, 1: 2}


def test_local_dataframe_io(write_jsonl, tmp_path) -> None:
    path = write_jsonl([{"a": 1}, {"a": 2}])
    df = LocalDataFrame().read_json(str(path))
    rows = df.collect()
    assert isinstance(rows[0], RowSchema)
    assert rows[0].data["a"] == 1

    out_path = tmp_path / "out.jsonl"
    LocalDataFrame(rows).write_json(str(out_path))
    lines = [json.loads(line) for line in out_path.read_text().splitlines()]
    assert lines[0]["type"] == "row"
    assert "data" in lines[0]


def test_local_dataframe_parquet_io(tmp_path) -> None:
    pytest.importorskip("pyarrow")

    first_path = tmp_path / "part1.parquet"
    second_path = tmp_path / "part2.parquet"

    LocalDataFrame([RowSchema(data={"a": 1})]).write_parquet(str(first_path))
    LocalDataFrame([RowSchema(data={"a": 2})]).write_parquet(str(second_path))

    df = LocalDataFrame().read_parquet([str(first_path), str(second_path)])
    values = [row.data["a"] for row in df.collect()]
    assert values == [1, 2]
