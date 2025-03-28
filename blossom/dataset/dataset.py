from typing import Callable, Optional
from abc import ABC, abstractmethod

from blossom.context.context import Context
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
    def execute(self, operators: list[Operator]) -> "Dataset":
        pass

    @abstractmethod
    def collect(self) -> list[Schema]:
        pass

    @abstractmethod
    def count(self) -> int:
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
