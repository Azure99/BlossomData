import json
import os
import random
from typing import Callable, Optional, Union

from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.default_data_handler import DefaultDataHandler
from blossom.log import logger
from blossom.schema.schema import Schema


class LocalDataFrame(DataFrame):
    def __init__(self, data: Optional[list[Schema]] = None):
        self.data = data or []

    def map(self, func: Callable[[Schema], Schema]) -> "LocalDataFrame":
        return LocalDataFrame([func(schema) for schema in self.data])

    def filter(self, func: Callable[[Schema], bool]) -> "LocalDataFrame":
        return LocalDataFrame([schema for schema in self.data if func(schema)])

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        return LocalDataFrame(func(self.data))

    def count(self) -> int:
        return len(self.data)

    def limit(self, num_rows: int) -> "DataFrame":
        return LocalDataFrame(self.data[:num_rows])

    def shuffle(self) -> "DataFrame":
        shuffled_data = self.data[:]
        random.shuffle(shuffled_data)
        return LocalDataFrame(shuffled_data)

    def repartition(self, num_partitions: int) -> "DataFrame":
        logger.warning("LocalDataFrame does not support repartition.")
        return LocalDataFrame(self.data)

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        return sum(func(schema) for schema in self.data)

    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        if not isinstance(others, list):
            others = [others]

        unioned_data = self.data[:]
        for other in others:
            assert isinstance(other, LocalDataFrame)
            unioned_data.extend(other.data)

        return LocalDataFrame(unioned_data)

    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        return LocalDataFrame(schemas)

    def collect(self) -> list[Schema]:
        return self.data

    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        file_list = self._list_files(path, [".json", ".jsonl"], True)
        data_handler = data_handler or DefaultDataHandler()

        rows = []
        for file in file_list:
            with open(file, encoding="utf-8") as f:
                rows.extend([json.loads(line) for line in f])
        return LocalDataFrame([data_handler.from_dict(row) for row in rows])

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        data_handler = data_handler or DefaultDataHandler()

        with open(path, "w", encoding="utf-8") as f:
            for schema in self.data:
                json_data = data_handler.to_dict(schema)
                f.write(json.dumps(json_data, ensure_ascii=False) + "\n")

    def _list_files(
        self, path: str, exts: list[str], include_no_ext: bool
    ) -> list[str]:
        if not os.path.isdir(path):
            return [path]

        file_list = []
        for file in os.listdir(path):
            file_lower = file.lower()
            if include_no_ext and "." not in file_lower:
                file_list.append(file)
            elif any(file_lower.endswith(ext) for ext in exts):
                file_list.append(file)
        return [os.path.join(path, file) for file in file_list]
