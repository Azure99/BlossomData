from __future__ import annotations

from typing import Any

from blossom.dataset import dataset as dataset_module


class _DummyDataset(dataset_module.Dataset):
    def _not_impl(self, *args: Any, **kwargs: Any):
        raise NotImplementedError

    def map(self, func):
        return self._not_impl()

    def filter(self, func):
        return self._not_impl()

    def transform(self, func):
        return self._not_impl()

    def sort(self, func, ascending: bool = True):
        return self._not_impl()

    def execute(self, operators):
        return self._not_impl()

    def collect(self):
        return self._not_impl()

    def limit(self, num_rows: int):
        return self._not_impl()

    def shuffle(self):
        return self._not_impl()

    def repartition(self, num_partitions: int):
        return self._not_impl()

    def split(self, n: int):
        return self._not_impl()

    def aggregate(self, *aggs):
        return self._not_impl()

    def group_by(self, func, name: str = "group"):
        return self._not_impl()

    def union(self, others):
        return self._not_impl()

    def cache(self):
        return self._not_impl()

    def from_list(self, schemas):
        return self._not_impl()

    def read_json(self, path, data_handler=None):
        return self._not_impl()

    def write_json(self, path, data_handler=None):
        return self._not_impl()

    def add_metadata(self, func):
        return self._not_impl()

    def drop_metadata(self, keys):
        return self._not_impl()

    def sum(self, func):
        return self._not_impl()

    def mean(self, func):
        return self._not_impl()

    def count(self):
        return self._not_impl()

    def min(self, func):
        return self._not_impl()

    def max(self, func):
        return self._not_impl()

    def variance(self, func):
        return self._not_impl()

    def stddev(self, func):
        return self._not_impl()

    def unique(self, func):
        return self._not_impl()


class _DummyGroupedDataset(dataset_module.GroupedDataset):
    def _not_impl(self, *args: Any, **kwargs: Any):
        raise NotImplementedError

    def aggregate(self, *aggs):
        return self._not_impl()

    def sum(self, func):
        return self._not_impl()

    def mean(self, func):
        return self._not_impl()

    def count(self):
        return self._not_impl()

    def min(self, func):
        return self._not_impl()

    def max(self, func):
        return self._not_impl()

    def variance(self, func):
        return self._not_impl()

    def stddev(self, func):
        return self._not_impl()

    def unique(self, func):
        return self._not_impl()


def test_dataset_uses_default_context(monkeypatch) -> None:
    class _DummyContext:
        pass

    monkeypatch.setattr(dataset_module, "Context", _DummyContext)
    dataset = _DummyDataset()
    assert isinstance(dataset.context, _DummyContext)


def test_dataset_uses_provided_context() -> None:
    context = object()
    dataset = _DummyDataset(context=context)
    assert dataset.context is context


def test_grouped_dataset_stores_context() -> None:
    context = object()
    dataset = _DummyGroupedDataset(context)
    assert dataset.context is context
