from __future__ import annotations

import pytest

from blossom.dataset import create_dataset, load_dataset


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
