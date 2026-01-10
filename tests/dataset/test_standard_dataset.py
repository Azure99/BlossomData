from __future__ import annotations

from pathlib import Path

from blossom.dataframe.aggregate import Sum
from blossom.dataset import DatasetEngine, create_dataset, load_dataset
from blossom.op.map_operator import map_operator
from blossom.schema.text_schema import TextSchema


@map_operator()
def _uppercase(item: TextSchema) -> TextSchema:
    item.content = item.content.upper()
    return item


def test_standard_dataset_execute_and_collect() -> None:
    data = [TextSchema(content="a"), TextSchema(content="bb")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    result = dataset.execute([_uppercase]).collect()
    assert [item.content for item in result] == ["A", "BB"]


def test_standard_dataset_aggregate_group_by() -> None:
    data = [TextSchema(content="a"), TextSchema(content="bb"), TextSchema(content="c")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    dataset = dataset.add_metadata(lambda x: {"len": len(x.content)})

    total = dataset.aggregate(Sum(lambda x: x.metadata["len"]))
    expected_total = 4
    assert total == expected_total

    grouped = dataset.group_by(lambda x: x.metadata["len"], name="len").count()
    rows = grouped.collect()
    grouped_counts = {row.data["len"]: row.data["count"] for row in rows}
    assert grouped_counts == {1: 2, 2: 1}


def test_standard_dataset_union_split_cache_and_io(tmp_path: Path) -> None:
    dataset1 = create_dataset(
        [TextSchema(content="a"), TextSchema(content="b")],
        engine=DatasetEngine.LOCAL,
    )
    dataset2 = create_dataset([TextSchema(content="c")], engine=DatasetEngine.LOCAL)

    unioned = dataset1.union(dataset2)
    assert [item.content for item in unioned.collect()] == ["a", "b", "c"]

    parts = unioned.split(2)
    assert sorted([len(part.collect()) for part in parts]) == [1, 2]

    cached = unioned.cache()
    assert [item.content for item in cached.collect()] == ["a", "b", "c"]

    json_path = tmp_path / "out.jsonl"
    unioned.write_json(str(json_path))
    reloaded = load_dataset(str(json_path), engine=DatasetEngine.LOCAL)
    assert [item.content for item in reloaded.collect()] == ["a", "b", "c"]
