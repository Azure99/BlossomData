import copy
import json
import os
import random
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
from typing import Any, Callable, Optional, Union, cast

import pyarrow as pa
import pyarrow.parquet as pq

import cloudpickle  # type: ignore[import-untyped]

from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler, DefaultDataHandler
from blossom.dataframe.dataframe import DataFrame, GroupedDataFrame
from blossom.log import logger
from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import FIELD_FAILURE_REASON, FIELD_METADATA, Schema


def _schemas_to_dicts(schemas: list[Schema]) -> list[dict[str, Any]]:
    return [schema.to_dict() for schema in schemas]


def _dicts_to_schemas(dicts: list[dict[str, Any]]) -> list[Schema]:
    return [Schema.from_dict(d) for d in dicts]


def _loads_callable(serialized_func: bytes) -> Callable[..., Any]:
    return cast(Callable[..., Any], cloudpickle.loads(serialized_func))


def _map_chunk(
    serialized_func: bytes, chunk: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    func = _loads_callable(serialized_func)
    return [func(Schema.from_dict(row)).to_dict() for row in chunk]


def _filter_chunk(
    serialized_func: bytes, chunk: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    func = _loads_callable(serialized_func)
    result: list[dict[str, Any]] = []
    for row in chunk:
        if func(Schema.from_dict(row)):
            result.append(row)
    return result


def _transform_chunk(
    serialized_func: bytes, chunk: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    func = _loads_callable(serialized_func)
    schemas = [Schema.from_dict(row) for row in chunk]
    transformed = func(schemas)
    return [schema.to_dict() for schema in transformed]


def _calc_key_chunk(
    serialized_func: bytes, chunk: list[dict[str, Any]]
) -> list[tuple[Any, dict[str, Any]]]:
    func = _loads_callable(serialized_func)
    return [(func(Schema.from_dict(row)), row) for row in chunk]


class MultiProcessDataFrame(DataFrame):
    def __init__(
        self,
        data: Optional[list[Schema]] = None,
        num_workers: Optional[int] = None,
    ):
        self.data = data or []
        self.num_workers = max(1, num_workers or (os.cpu_count() or 1))

    def _partition(self, n_parts: int) -> list[list[Schema]]:
        total = len(self.data)
        if total == 0:
            return [[] for _ in range(n_parts)]
        chunk = total // n_parts
        rem = total % n_parts
        res: list[list[Schema]] = []
        start = 0
        for i in range(n_parts):
            end = start + chunk + (1 if i < rem else 0)
            res.append(self.data[start:end])
            start = end
        return res

    def map(self, func: Callable[[Schema], Schema]) -> "MultiProcessDataFrame":
        if len(self.data) == 0:
            return MultiProcessDataFrame([], self.num_workers)

        parts = self._partition(self.num_workers)
        dict_parts = [_schemas_to_dicts(p) for p in parts]
        serialized_func = cloudpickle.dumps(func)

        results: list[list[dict[str, Any]]] = []
        with ProcessPoolExecutor(max_workers=self.num_workers) as ex:
            for res in ex.map(_map_chunk, repeat(serialized_func), dict_parts):
                results.append(res)

        flattened = [row for part in results for row in part]
        return MultiProcessDataFrame(_dicts_to_schemas(flattened), self.num_workers)

    def filter(self, func: Callable[[Schema], bool]) -> "MultiProcessDataFrame":
        if len(self.data) == 0:
            return MultiProcessDataFrame([], self.num_workers)

        parts = self._partition(self.num_workers)
        dict_parts = [_schemas_to_dicts(p) for p in parts]
        serialized_func = cloudpickle.dumps(func)

        results: list[list[dict[str, Any]]] = []
        with ProcessPoolExecutor(max_workers=self.num_workers) as ex:
            for res in ex.map(_filter_chunk, repeat(serialized_func), dict_parts):
                results.append(res)

        flattened = [row for part in results for row in part]
        return MultiProcessDataFrame(_dicts_to_schemas(flattened), self.num_workers)

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        if len(self.data) == 0:
            return MultiProcessDataFrame([], self.num_workers)

        parts = self._partition(self.num_workers)
        dict_parts = [_schemas_to_dicts(p) for p in parts]
        serialized_func = cloudpickle.dumps(func)

        results: list[list[dict[str, Any]]] = []
        with ProcessPoolExecutor(max_workers=self.num_workers) as ex:
            for res in ex.map(_transform_chunk, repeat(serialized_func), dict_parts):
                results.append(res)

        flattened = [row for part in results for row in part]
        return MultiProcessDataFrame(_dicts_to_schemas(flattened), self.num_workers)

    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        if len(self.data) == 0:
            return MultiProcessDataFrame([], self.num_workers)

        parts = self._partition(self.num_workers)
        dict_parts = [_schemas_to_dicts(p) for p in parts]

        keyed_parts: list[list[tuple[Any, dict[str, Any]]]] = []
        serialized_func = cloudpickle.dumps(func)

        with ProcessPoolExecutor(max_workers=self.num_workers) as ex:
            for res in ex.map(_calc_key_chunk, repeat(serialized_func), dict_parts):
                keyed_parts.append(res)

        keyed_all = [kv for part in keyed_parts for kv in part]
        keyed_all.sort(key=lambda x: x[0], reverse=not ascending)
        sorted_rows = [row for _, row in keyed_all]
        return MultiProcessDataFrame(_dicts_to_schemas(sorted_rows), self.num_workers)

    def limit(self, num_rows: int) -> "DataFrame":
        return MultiProcessDataFrame(self.data[:num_rows], self.num_workers)

    def shuffle(self) -> "DataFrame":
        shuffled = self.data[:]
        random.shuffle(shuffled)
        return MultiProcessDataFrame(shuffled, self.num_workers)

    def repartition(self, num_partitions: int) -> "DataFrame":
        if num_partitions < 1:
            logger.warning("num_partitions must be >= 1; keeping previous value")
            return MultiProcessDataFrame(self.data, self.num_workers)
        return MultiProcessDataFrame(self.data, num_partitions)

    def split(self, n: int) -> list["DataFrame"]:
        total = len(self.data)
        chunk_size = total // n
        remainder = total % n
        start = 0
        dataframes: list[DataFrame] = []
        for i in range(n):
            end = start + chunk_size + (1 if i < remainder else 0)
            dataframes.append(
                MultiProcessDataFrame(self.data[start:end], self.num_workers)
            )
            start = end
        return dataframes

    def aggregate(
        self,
        *aggs: AggregateFunc,
    ) -> Union[Any, dict[str, Any]]:
        # Aggregations can close over non-picklable state, so execute them in-process.
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
        return GroupedMultiProcessDataFrame(name, grouped_data, self.num_workers)

    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        if not isinstance(others, list):
            others = [others]

        union_data = self.data[:]
        for other in others:
            assert isinstance(other, MultiProcessDataFrame)
            union_data.extend(other.data)

        return MultiProcessDataFrame(union_data, self.num_workers)

    def cache(self) -> "DataFrame":
        return MultiProcessDataFrame(copy.deepcopy(self.data), self.num_workers)

    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        return MultiProcessDataFrame(schemas, self.num_workers)

    def collect(self) -> list[Schema]:
        return self.data

    def read_json(
        self, path: Union[str, list[str]], data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        paths = [path] if isinstance(path, str) else path
        data_handler = data_handler or DefaultDataHandler()

        rows = []
        for p in paths:
            files = self._list_files(p, [".json", ".jsonl"], True)
            for file in files:
                with open(file, encoding="utf-8") as f:
                    rows.extend([json.loads(line) for line in f])

        return MultiProcessDataFrame(
            [data_handler.from_dict(row) for row in rows],
            self.num_workers,
        )

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        data_handler = data_handler or DefaultDataHandler()

        with open(path, "w", encoding="utf-8") as f:
            for schema in self.data:
                json_data = data_handler.to_dict(schema)
                f.write(json.dumps(json_data, ensure_ascii=False) + "\n")

    def read_parquet(
        self, path: Union[str, list[str]], data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        paths = [path] if isinstance(path, str) else path
        data_handler = data_handler or DefaultDataHandler()

        rows = []
        for single_path in paths:
            table = pq.read_table(single_path)
            rows.extend(table.to_pylist())

        return MultiProcessDataFrame(
            [data_handler.from_dict(row) for row in rows],
            self.num_workers,
        )

    def write_parquet(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> None:
        data_handler = data_handler or DefaultDataHandler()
        dict_rows: list[dict[str, Any]] = []
        for schema in self.data:
            row = data_handler.to_dict(schema)
            metadata = row.get(FIELD_METADATA)
            if metadata is None or metadata == {}:
                row.pop(FIELD_METADATA, None)
            dict_rows.append(row)
        table = pa.Table.from_pylist(dict_rows)
        pq.write_table(table, path)

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


class GroupedMultiProcessDataFrame(GroupedDataFrame):
    def __init__(
        self,
        name: str,
        grouped_data: dict[Any, list[Schema]],
        num_workers: int,
    ):
        super().__init__(name)
        self.grouped_data = grouped_data
        self.num_workers = num_workers

    def aggregate(self, *aggs: AggregateFunc) -> DataFrame:
        # Keep group aggregations local to avoid pickling user-defined closures.
        grouped_results: list[Schema] = []

        for group_value, data in self.grouped_data.items():
            result_row = RowSchema(data={self.name: group_value})
            for agg in aggs:
                result = agg.initial_value
                for schema in data:
                    result = agg.accumulate(result, schema)
                result_row[agg.name] = agg.finalize(result)
            grouped_results.append(result_row)

        return MultiProcessDataFrame(grouped_results, self.num_workers)
