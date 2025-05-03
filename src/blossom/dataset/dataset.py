from typing import Any, Callable, Optional, Union
from abc import ABC, abstractmethod

from blossom.context.context import Context
from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class Dataset(ABC):
    def __init__(self, context: Optional[Context] = None):
        self.context = context or Context()

    @abstractmethod
    def map(self, func: Callable[[Schema], Schema]) -> "Dataset":
        pass

    @abstractmethod
    def filter(self, func: Callable[[Schema], bool]) -> "Dataset":
        pass

    @abstractmethod
    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "Dataset":
        pass

    @abstractmethod
    def sort(self, func: Callable[[Schema], Any], ascending: bool = True) -> "Dataset":
        pass

    @abstractmethod
    def execute(self, operators: list[Operator]) -> "Dataset":
        pass

    @abstractmethod
    def collect(self) -> list[Schema]:
        pass

    @abstractmethod
    def limit(self, num_rows: int) -> "Dataset":
        pass

    @abstractmethod
    def shuffle(self) -> "Dataset":
        pass

    @abstractmethod
    def repartition(self, num_partitions: int) -> "Dataset":
        pass

    @abstractmethod
    def split(self, n: int) -> list["Dataset"]:
        pass

    @abstractmethod
    def aggregate(self, *aggs: AggregateFunc) -> Union[Any, dict[str, Any]]:
        pass

    @abstractmethod
    def group_by(
        self, func: Callable[[Schema], Any], name: str = "group"
    ) -> "GroupedDataset":
        pass

    @abstractmethod
    def union(self, others: Union["Dataset", list["Dataset"]]) -> "Dataset":
        pass

    @abstractmethod
    def cache(self) -> "Dataset":
        pass

    @abstractmethod
    def from_list(self, schemas: list[Schema]) -> "Dataset":
        pass

    @abstractmethod
    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "Dataset":
        pass

    @abstractmethod
    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        pass

    @abstractmethod
    def add_metadata(self, func: Callable[[Schema], dict[str, Any]]) -> "Dataset":
        pass

    @abstractmethod
    def drop_metadata(self, keys: list[str]) -> "Dataset":
        pass

    @abstractmethod
    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def mean(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def min(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def max(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def variance(
        self, func: Callable[[Schema], Union[int, float]]
    ) -> Union[int, float]:
        pass

    @abstractmethod
    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def unique(self, func: Callable[[Schema], set[Any]]) -> list[Any]:
        pass


class GroupedDataset(ABC):
    def __init__(self, context: Context):
        self.context = context

    @abstractmethod
    def aggregate(self, *aggs: AggregateFunc) -> "Dataset":
        pass

    @abstractmethod
    def sum(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def mean(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def count(self) -> "Dataset":
        pass

    @abstractmethod
    def min(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def max(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def variance(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> "Dataset":
        pass

    @abstractmethod
    def unique(self, func: Callable[[Schema], set[Any]]) -> "Dataset":
        pass
