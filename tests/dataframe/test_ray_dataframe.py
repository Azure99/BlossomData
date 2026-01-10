from __future__ import annotations

import json

import pytest

ray = pytest.importorskip("ray")
pytest.importorskip("pyarrow")

from blossom.dataframe.aggregate import Count, Sum
from blossom.dataframe.ray_dataframe import RayDataFrame
from blossom.schema.row_schema import RowSchema

pytestmark = pytest.mark.ray


@pytest.fixture(scope="module")
def ray_context() -> None:
    created = False
    if not ray.is_initialized():
        ray.init(
            num_cpus=2,
            include_dashboard=False,
            ignore_reinit_error=True,
            log_to_driver=False,
        )
        created = True
    yield
    if created:
        ray.shutdown()


def _make_df(values: list[int], start_idx: int = 0) -> RayDataFrame:
    base = RayDataFrame()
    rows = [
        RowSchema(data={"idx": start_idx + idx, "value": value})
        for idx, value in enumerate(values)
    ]
    return base.from_list(rows)  # type: ignore[return-value]


def _values(items: list[RowSchema]) -> list[int]:
    return [int(item.data["value"]) for item in items]


def _collect_sorted_by_idx(dataframe: RayDataFrame) -> list[RowSchema]:
    return dataframe.sort(lambda s: s.data["idx"]).collect()


def test_ray_dataframe_basic_ops(ray_context: None) -> None:
    df = _make_df([1, 2, 3])

    mapped = df.map(
        lambda s: RowSchema(
            data={"idx": s.data["idx"], "value": s.data["value"] + 1}
        )
    )
    assert _values(_collect_sorted_by_idx(mapped)) == [2, 3, 4]

    filtered = df.filter(lambda s: s.data["value"] > 1)
    assert _values(_collect_sorted_by_idx(filtered)) == [2, 3]

    transformed = df.transform(
        lambda items: [
            RowSchema(data={"idx": s.data["idx"], "value": s.data["value"] * 2})
            for s in items
        ]
    )
    assert _values(_collect_sorted_by_idx(transformed)) == [2, 4, 6]

    sorted_df = df.sort(lambda s: s.data["value"], ascending=False)
    assert _values(sorted_df.collect()) == [3, 2, 1]

    limited = df.sort(lambda s: s.data["idx"]).limit(2)
    assert _values(limited.sort(lambda s: s.data["idx"]).collect()) == [1, 2]

    shuffled = df.shuffle()
    assert _values(_collect_sorted_by_idx(shuffled)) == [1, 2, 3]

    repartitioned = df.repartition(2)
    assert _values(_collect_sorted_by_idx(repartitioned)) == [1, 2, 3]

    splits = df.split(2)
    assert [len(s.collect()) for s in splits] == [2, 1]

    unioned = df.union(_make_df([4], start_idx=3))
    assert _values(_collect_sorted_by_idx(unioned)) == [1, 2, 3, 4]

    cached = df.cache()
    assert _values(_collect_sorted_by_idx(cached)) == [1, 2, 3]

    total = df.aggregate(Sum(lambda s: s.data["value"]))
    assert total == 6

    multi = df.aggregate(Sum(lambda s: s.data["value"]), Count())
    assert multi == {"sum": 6, "count": 3}

    grouped = df.group_by(lambda s: s.data["value"] % 2, name="parity").count()
    grouped_rows = grouped.sort(lambda s: s.data["parity"]).collect()
    grouped_map = {row.data["parity"]: row.data["count"] for row in grouped_rows}
    assert grouped_map == {0: 1, 1: 2}

    base = RayDataFrame()
    from_list_df = base.from_list([RowSchema(data={"idx": 0, "value": 7})])
    assert _values(_collect_sorted_by_idx(from_list_df)) == [7]


def test_ray_dataframe_io(
    ray_context: None, write_jsonl, tmp_path
) -> None:
    path = write_jsonl([{"a": 1}, {"a": 2}])
    df = RayDataFrame().read_json(str(path))
    rows = df.collect()
    assert all(isinstance(row, RowSchema) for row in rows)
    assert sorted(row.data["a"] for row in rows) == [1, 2]

    out_path = tmp_path / "ray_out"
    df.write_json(str(out_path))

    files = [p for p in out_path.rglob("*") if p.is_file()]
    lines: list[str] = []
    for part in files:
        if part.stat().st_size == 0:
            continue
        lines.extend(part.read_text().splitlines())

    payloads = []
    for line in lines:
        if not line.strip():
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict) and "data" in data and "type" in data:
            payloads.append(data)

    assert payloads
    values = sorted(item["data"]["a"] for item in payloads)
    assert values == [1, 2]
    assert all(item["type"] == "row" for item in payloads)
