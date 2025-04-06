from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Union

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
    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        pass

    def add_metadata(self, func: Callable[[Schema], dict[str, Any]]) -> "DataFrame":
        def add_metadata_to_schema(schema: Schema) -> Schema:
            schema.metadata.update(func(schema))
            return schema

        return self.map(add_metadata_to_schema)

    def drop_metadata(self, keys: list[str]) -> "DataFrame":
        def drop_metadata_from_schema(schema: Schema) -> Schema:
            for key in keys:
                schema.metadata.pop(key, None)
            return schema

        return self.map(drop_metadata_from_schema)

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
    def split(self, n: int) -> list["DataFrame"]:
        pass

    @abstractmethod
    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        pass

    @abstractmethod
    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        pass

    @abstractmethod
    def cache(self) -> "DataFrame":
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
