from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional

from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class MetricFilterOperator(Operator, ABC):
    """
    Base class for filters that rely on expensive metrics computed per item.

    A metric filter behaves like a normal `FilterOperator`, but it first ensures
    the required metrics are available (reusing cached values when possible)
    before delegating to `should_keep`.
    """

    def __init__(
        self,
        metrics_metadata_key: Optional[str] = None,
        recompute_metrics: bool = False,
        metrics_only: bool = False,
        reverse: bool = False,
        parallel: int = 1,
    ):
        """
        Initialize the metric filter with caching and execution options.

        Args:
            metrics_metadata_key: Metadata key where computed metrics are stored.
            recompute_metrics: Force recomputation even if metrics exist.
            metrics_only: Skip filtering, only populate metrics metadata.
            reverse: If True, keep items where the predicate returns False (default: False)
            parallel: Number of parallel threads to use (default: 1)
        """
        super().__init__()
        self.metrics_metadata_key = (
            metrics_metadata_key or self._default_metrics_metadata_key()
        )
        self.recompute_metrics = recompute_metrics
        self.metrics_only = metrics_only
        self.reverse = reverse
        self.parallel = parallel

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Populate metrics first, then filter the dataframe if requested.
        """
        if self.parallel > 1:
            dataframe = dataframe.transform(self._parallel_map(self._ensure_metrics))
        else:
            dataframe = dataframe.map(self._ensure_metrics)

        if self.metrics_only:
            return dataframe

        if self.parallel > 1:
            return dataframe.transform(self._parallel_filter(self._evaluate_keep))
        return dataframe.filter(lambda item: self._evaluate_keep(item) ^ self.reverse)

    def _ensure_metrics(self, item: Schema) -> Schema:
        if item.failed:
            return item

        if self.metrics_metadata_key in item.metadata:
            if not self.recompute_metrics:
                return item

        item.metadata[self.metrics_metadata_key] = self.compute_metrics(item)
        return item

    def _evaluate_keep(self, item: Schema) -> bool:
        if item.failed:
            return True
        metrics = item.metadata[self.metrics_metadata_key]
        return self.should_keep(item, metrics)

    def _parallel_map(
        self, func: Callable[[Schema], Schema]
    ) -> Callable[[list[Schema]], list[Schema]]:
        def batch_map(data: list[Schema]) -> list[Schema]:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                return list(executor.map(func, data))

        return batch_map

    def _parallel_filter(
        self, predicate: Callable[[Schema], bool]
    ) -> Callable[[list[Schema]], list[Schema]]:
        def batch_filter(data: list[Schema]) -> list[Schema]:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                results = list(executor.map(predicate, data))
                return [
                    item for item, passed in zip(data, results) if passed ^ self.reverse
                ]

        return batch_filter

    def _default_metrics_metadata_key(self) -> str:
        return f"{self.__class__.__name__}_metrics"

    @abstractmethod
    def compute_metrics(self, item: Schema) -> Any:
        """
        Compute metrics for an item.

        Implementations should return an object that can be cached in metadata.
        """

    @abstractmethod
    def should_keep(self, item: Schema, metrics: Any) -> bool:
        """
        Decide whether the item is kept, given the pre-computed metrics.

        Return True to keep the item (prior to `reverse` being applied).
        """
