from abc import ABC, abstractmethod
from typing import Any, Optional

from blossom.op.filter_operator import FilterOperator
from blossom.schema.schema import Schema


class MetricFilterOperator(FilterOperator, ABC):
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
            reverse: Pass-through to `FilterOperator` to invert keep logic.
            parallel: Pass-through to `FilterOperator` to set thread count.
        """
        super().__init__(reverse=reverse, parallel=parallel)
        self.metrics_metadata_key = (
            metrics_metadata_key or self._default_metrics_metadata_key()
        )
        self.recompute_metrics = recompute_metrics
        self.metrics_only = metrics_only

    def process_item(self, item: Schema) -> bool:
        """
        Ensure metrics are present for `item` and evaluate keep logic.

        When `metrics_only` is True, metrics are still computed but the item is
        always kept so downstream operators can rely on the new metadata.
        """
        metrics = self._get_or_compute_metrics(item)
        if not self.metrics_only:
            return self.should_keep(item, metrics)

        # When only metrics are requested, still compute them but keep all items.
        return True

    def _get_or_compute_metrics(self, item: Schema) -> Any:
        """
        Fetch cached metrics or compute and cache them when missing/stale.

        Returns:
            The metrics object that should be passed to `should_keep`.
        """
        if self.metrics_metadata_key in item.metadata and not self.recompute_metrics:
            return item.metadata[self.metrics_metadata_key]

        metrics = self.compute_metrics(item)
        item.metadata[self.metrics_metadata_key] = metrics
        return metrics

    def _default_metrics_metadata_key(self) -> str:
        """Provide a deterministic default cache key based on the class name."""
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
