from typing import Callable, Optional, Union, Any
import numpy as np
from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


DEFAULT_BIN_FIELD = "bin"
DEFAULT_BIN_LABEL_FIELD = "bin_label"


class EqualWidthBinner(Operator):
    def __init__(
        self,
        func: Callable[[Schema], Union[int, float]],
        num_bins: int = 10,
        labels: Optional[list[Any]] = None,
        range_min: Optional[float] = None,
        range_max: Optional[float] = None,
        bin_field: str = DEFAULT_BIN_FIELD,
        bin_label_field: str = DEFAULT_BIN_LABEL_FIELD,
    ):
        super().__init__()
        self.func = func
        self.num_bins = num_bins
        self.labels = labels
        self.range_min = range_min
        self.range_max = range_max
        self.bin_field = bin_field
        self.bin_label_field = bin_label_field
        if labels and len(labels) != num_bins:
            raise ValueError(f"Number of labels must match number of bins: {num_bins}")

    def _compute_bin_edges(self, dataframe: DataFrame) -> list[float]:
        range_min = self.range_min or dataframe.cache().min(self.func)
        range_max = self.range_max or dataframe.cache().max(self.func)
        bin_edges = np.linspace(range_min, range_max, self.num_bins + 1).tolist()
        assert isinstance(bin_edges, list)
        return bin_edges

    def _gen_bin_labels(self, bin_edges: list[float]) -> list[Any]:
        if self.labels:
            return self.labels

        labels = []
        for i in range(len(bin_edges) - 1):
            labels.append(bin_edges[i])

        return labels

    def _get_value_bin_index(
        self, value: Union[int, float], bin_edges: list[float]
    ) -> int:
        index = int(np.digitize(value, bin_edges) - 1)
        return max(0, min(index, len(bin_edges) - 2))

    def _get_value_bin_label(self, index: int, bin_labels: list[Any]) -> Any:
        return bin_labels[index]

    def process(self, dataframe: DataFrame) -> DataFrame:
        bin_edges = self._compute_bin_edges(dataframe)
        bin_labels = self._gen_bin_labels(bin_edges)

        def _map_item(item: Schema) -> Schema:
            val = self.func(item)
            bin_index = self._get_value_bin_index(val, bin_edges)
            bin_label = self._get_value_bin_label(bin_index, bin_labels)
            item.metadata[self.bin_field] = bin_index
            item.metadata[self.bin_label_field] = bin_label
            return item

        return dataframe.map(_map_item)
