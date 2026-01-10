from __future__ import annotations

from blossom.dataset import DatasetEngine, create_dataset, load_dataset
from blossom.schema.text_schema import TextSchema


def test_create_dataset_local() -> None:
    data = [TextSchema(content="a"), TextSchema(content="b")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    assert [item.content for item in dataset.collect()] == ["a", "b"]


def test_load_dataset_local_json(write_jsonl) -> None:
    path = write_jsonl(
        [
            {"type": "text", "content": "hello"},
            {"type": "text", "content": "world"},
        ]
    )
    dataset = load_dataset(str(path), engine=DatasetEngine.LOCAL)
    result = dataset.collect()
    assert [item.content for item in result] == ["hello", "world"]
