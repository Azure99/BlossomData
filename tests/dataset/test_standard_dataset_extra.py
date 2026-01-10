from __future__ import annotations

from pathlib import Path

from blossom.dataset import DatasetEngine, create_dataset, load_dataset
from blossom.schema.text_schema import TextSchema


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
