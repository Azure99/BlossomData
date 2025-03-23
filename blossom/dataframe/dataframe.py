from abc import ABC, abstractmethod
from typing import Callable, Optional, Union

from blossom.dataframe.data_handler import DataHandler
from blossom.schema.schema import Schema


class DataFrame(ABC):
    @abstractmethod
    def map(self, func: Callable[[Schema], Schema]) -> "DataFrame":
        pass

    @abstractmethod
    def filter(self, func: Callable[[Schema], bool]) -> "DataFrame":
        pass

    @abstractmethod
    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        pass

    @abstractmethod
    def collect(self) -> list[Schema]:
        pass

    @abstractmethod
    def count(self) -> int:
        pass

    @abstractmethod
    def limit(self, num_rows: int) -> "DataFrame":
        pass

    @abstractmethod
    def shuffle(self) -> "DataFrame":
        pass

    @abstractmethod
    def repartition(self, num_partitions: int) -> "DataFrame":
        pass

    @abstractmethod
    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        pass

    @abstractmethod
    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        pass

    @abstractmethod
    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        pass
