from __future__ import annotations

import pytest

from blossom.dataset import DataType, DatasetEngine, create_dataset, load_dataset
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


def test_load_dataset_local_parquet(tmp_path) -> None:
    pytest.importorskip("pyarrow")

    data = [TextSchema(content="hello"), TextSchema(content="world")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    path = tmp_path / "data.parquet"
    dataset.write_parquet(str(path))

    loaded = load_dataset(
        str(path),
        engine=DatasetEngine.LOCAL,
        data_type=DataType.PARQUET,
    )
    result = loaded.collect()
    assert [item.content for item in result] == ["hello", "world"]


def test_load_dataset_invalid_engine() -> None:
    with pytest.raises(ValueError, match="Invalid dataset engine"):
        load_dataset("/tmp/nowhere", engine="invalid")  # type: ignore[arg-type]


def test_load_dataset_invalid_type(tmp_path) -> None:
    path = tmp_path / "data.jsonl"
    path.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Invalid file type"):
        load_dataset(str(path), data_type="invalid")  # type: ignore[arg-type]


def test_create_dataset_invalid_engine() -> None:
    with pytest.raises(ValueError, match="Invalid dataset engine"):
        create_dataset(engine="invalid")  # type: ignore[arg-type]
