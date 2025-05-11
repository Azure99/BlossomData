import copy
import json
import os
import random
from typing import Any, Callable, Optional, Union

from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame, GroupedDataFrame
from blossom.dataframe.default_data_handler import DefaultDataHandler
from blossom.log import logger
from blossom.schema.row_schema import RowSchema
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

    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        return LocalDataFrame(sorted(self.data, key=func, reverse=not ascending))

    def limit(self, num_rows: int) -> "DataFrame":
        return LocalDataFrame(self.data[:num_rows])

    def shuffle(self) -> "DataFrame":
        shuffled_data = self.data[:]
        random.shuffle(shuffled_data)
        return LocalDataFrame(shuffled_data)

    def repartition(self, num_partitions: int) -> "DataFrame":
        logger.warning("LocalDataFrame does not support repartition.")
        return LocalDataFrame(self.data)

    def split(self, n: int) -> list["DataFrame"]:
        total = len(self.data)
        chunk_size = total // n
        remainder = total % n

        start = 0
        dataframes: list[DataFrame] = []
        for i in range(n):
            end = start + chunk_size + (1 if i < remainder else 0)
            dataframes.append(LocalDataFrame(self.data[start:end]))
            start = end
        return dataframes

    def aggregate(
        self,
        *aggs: AggregateFunc,
    ) -> Union[Any, dict[str, Any]]:
        results = {}
        for agg in aggs:
            result = agg.initial_value
            for schema in self.data:
                result = agg.accumulate(result, schema)
            results[agg.name] = agg.finalize(result)
        return results if len(aggs) > 1 else results[aggs[0].name]

    def group_by(
        self, func: Callable[[Schema], Any], name: str = "group"
    ) -> "GroupedDataFrame":
        grouped_data: dict[Any, list[Schema]] = {}
        for schema in self.data:
            key = func(schema)
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(schema)
        return GroupedLocalDataFrame(name, grouped_data)

    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        if not isinstance(others, list):
            others = [others]

        union_data = self.data[:]
        for other in others:
            assert isinstance(other, LocalDataFrame)
            union_data.extend(other.data)

        return LocalDataFrame(union_data)

    def cache(self) -> "DataFrame":
        return LocalDataFrame(copy.deepcopy(self.data))

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

    @staticmethod
    def _list_files(
        path: str, extensions: list[str], include_no_ext: bool
    ) -> list[str]:
        if not os.path.isdir(path):
            return [path]

        file_list = []
        for file in os.listdir(path):
            file_lower = file.lower()
            if include_no_ext and "." not in file_lower:
                file_list.append(file)
            elif any(file_lower.endswith(ext) for ext in extensions):
                file_list.append(file)
        return [os.path.join(path, file) for file in file_list]


class GroupedLocalDataFrame(GroupedDataFrame):
    def __init__(self, name: str, grouped_data: dict[Any, list[Schema]]):
        super().__init__(name)
        self.grouped_data = grouped_data

    def aggregate(self, *aggs: AggregateFunc) -> DataFrame:
        grouped_results: list[Schema] = []

        for group_value, data in self.grouped_data.items():
            result_row = RowSchema(data={self.name: group_value})
            for agg in aggs:
                result = agg.initial_value
                for schema in data:
                    result = agg.accumulate(result, schema)
                result_row[agg.name] = agg.finalize(result)
            grouped_results.append(result_row)

        return LocalDataFrame(grouped_results)
