from typing import Any, Callable, Optional, Union

from blossom.context.context import Context
from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame, GroupedDataFrame
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.dataset.dataset import Dataset, GroupedDataset
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class StandardDataset(Dataset):
    def __init__(
        self,
        context: Optional[Context] = None,
        dataframe: Optional[DataFrame] = None,
    ):
        super().__init__(context)
        self.dataframe = dataframe or LocalDataFrame()

    def map(self, func: Callable[[Schema], Schema]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.map(func))

    def filter(self, func: Callable[[Schema], bool]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.filter(func))

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.transform(func))

    def sort(self, func: Callable[[Schema], Any], ascending: bool = True) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.sort(func, ascending))

    def add_metadata(self, func: Callable[[Schema], dict[str, Any]]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.add_metadata(func))

    def drop_metadata(self, keys: list[str]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.drop_metadata(keys))

    def execute(self, operators: list[Operator]) -> "Dataset":
        current_df = self.dataframe
        for operator in operators:
            operator.init_context(self.context)
            current_df = operator.process(current_df)
        return StandardDataset(self.context, current_df)

    def collect(self) -> list[Schema]:
        return self.dataframe.collect()

    def limit(self, num_rows: int) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.limit(num_rows))

    def shuffle(self) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.shuffle())

    def repartition(self, num_partitions: int) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.repartition(num_partitions))

    def split(self, n: int) -> list["Dataset"]:
        return [
            StandardDataset(self.context, dataframe)
            for dataframe in self.dataframe.split(n)
        ]

    def aggregate(self, *aggs: AggregateFunc) -> Union[Any, dict[str, Any]]:
        return self.dataframe.aggregate(*aggs)

    def group_by(
        self, func: Callable[[Schema], Any], name: str = "group"
    ) -> "GroupedDataset":
        return GroupedStandardDataset(self.context, self.dataframe.group_by(func, name))

    def union(self, others: Union["Dataset", list["Dataset"]]) -> "Dataset":
        if not isinstance(others, list):
            others = [others]

        dataframes = []
        for dataset in others:
            assert isinstance(dataset, StandardDataset)
            dataframes.append(dataset.dataframe)

        return StandardDataset(self.context, self.dataframe.union(dataframes))

    def cache(self) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.cache())

    def from_list(self, schemas: list[Schema]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.from_list(schemas))

    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "Dataset":
        return StandardDataset(
            self.context, self.dataframe.read_json(path, data_handler)
        )

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        self.dataframe.write_json(path, data_handler)

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return self.dataframe.sum(func)

    def mean(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return self.dataframe.mean(func)

    def count(self) -> int:
        return self.dataframe.count()

    def min(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return self.dataframe.min(func)

    def max(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return self.dataframe.max(func)

    def variance(
        self, func: Callable[[Schema], Union[int, float]]
    ) -> Union[int, float]:
        return self.dataframe.variance(func)

    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return self.dataframe.stddev(func)

    def unique(self, func: Callable[[Schema], set[Any]]) -> list[Any]:
        return self.dataframe.unique(func)


class GroupedStandardDataset(GroupedDataset):
    def __init__(
        self,
        context: Context,
        grouped_dataframe: GroupedDataFrame,
    ):
        super().__init__(context)
        self.grouped_dataframe = grouped_dataframe

    def aggregate(self, *aggs: AggregateFunc) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.aggregate(*aggs))

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.sum(func))

    def mean(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.mean(func))

    def count(self) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.count())

    def min(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.min(func))

    def max(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.max(func))

    def variance(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.variance(func))

    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.stddev(func))

    def unique(self, func: Callable[[Schema], set[Any]]) -> "Dataset":
        return StandardDataset(self.context, self.grouped_dataframe.unique(func))
