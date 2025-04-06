import json
from typing import Callable, Iterator, Optional, Any, Union

import numpy as np
import pyarrow as pa
import ray
import ray.data
from ray.data.block import BlockAccessor
from ray.data.datasource import BlockBasedFileDatasink


from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.default_data_handler import DefaultDataHandler
from blossom.schema.schema import (
    FIELD_DATA,
    FIELD_FAILED,
    FIELD_ID,
    FIELD_METADATA,
    FIELD_TYPE,
    Schema,
)


SORT_KEY = "__sort_key__"
SUM_KEY = "__sum_key__"


def schema_to_row(schema: Schema) -> dict[str, Any]:
    data = schema.to_dict()
    row = {
        FIELD_ID: data.pop(FIELD_ID),
        FIELD_FAILED: data.pop(FIELD_FAILED),
        FIELD_TYPE: data.pop(FIELD_TYPE),
        FIELD_METADATA: json.dumps(data.pop(FIELD_METADATA), ensure_ascii=False),
    }
    row[FIELD_DATA] = json.dumps(data, ensure_ascii=False)
    return row


def row_to_schema(row: dict[str, Any]) -> Schema:
    data = json.loads(row[FIELD_DATA])
    data[FIELD_ID] = row[FIELD_ID]
    data[FIELD_FAILED] = row[FIELD_FAILED]
    data[FIELD_TYPE] = row[FIELD_TYPE]
    data[FIELD_METADATA] = json.loads(row[FIELD_METADATA])
    return Schema.from_dict(data)


def _map_batches(
    dataset: ray.data.Dataset,
    func: Callable[[list[dict[str, Any]]], list[dict[str, Any]]],
) -> ray.data.Dataset:
    def _map_batch(
        batch: dict[str, np.ndarray[Any, Any]]
    ) -> dict[str, np.ndarray[Any, Any]]:
        batch_size = len(next(iter(batch.values())))
        rows = [{k: v[i] for k, v in batch.items()} for i in range(batch_size)]
        transformed_rows = func(rows)

        result: dict[str, list[Any]] = {}
        for row in transformed_rows:
            for k, v in row.items():
                result.setdefault(k, []).append(v)
        return {k: np.array(v) for k, v in result.items()}

    result: ray.data.Dataset = dataset.map_batches(_map_batch)
    return result


class SchemaRowDatasink(BlockBasedFileDatasink):
    def __init__(self, path: str, data_handler: DataHandler):
        super().__init__(path, file_format="jsonl")
        self.data_handler = data_handler

    def write_block_to_file(self, block: BlockAccessor, file: pa.NativeFile) -> None:
        rows: Iterator[dict[str, Any]] = block.iter_rows(public_row_format=False)
        for row in rows:
            schema = row_to_schema(row)
            data_dict = self.data_handler.to_dict(schema)
            json_line = json.dumps(data_dict, ensure_ascii=False) + "\n"
            file.write(json_line.encode("utf-8"))


class RayDataFrame(DataFrame):
    def __init__(
        self,
        ray_dataset: Optional[ray.data.Dataset] = None,
    ):
        if not ray.is_initialized():
            ray.init()
        self.ray_dataset = ray_dataset or ray.data.from_items([])

    def map(self, func: Callable[[Schema], Schema]) -> "RayDataFrame":
        def map_row(row: dict[str, Any]) -> dict[str, Any]:
            schema = row_to_schema(row)
            return schema_to_row(func(schema))

        mapped_dataset = self.ray_dataset.map(map_row)
        return RayDataFrame(mapped_dataset)

    def filter(self, func: Callable[[Schema], bool]) -> "RayDataFrame":
        def filter_row(row: dict[str, Any]) -> bool:
            schema = row_to_schema(row)
            return func(schema)

        filtered_dataset = self.ray_dataset.filter(filter_row)
        return RayDataFrame(filtered_dataset)

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        def transform_partition(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
            transformed_schemas = func([row_to_schema(row) for row in batch])
            return [schema_to_row(schema) for schema in transformed_schemas]

        transformed_dataset = _map_batches(self.ray_dataset, transform_partition)
        return RayDataFrame(transformed_dataset)

    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        def calc_sort_key(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{**row, SORT_KEY: func(row_to_schema(row))} for row in batch]

        sorted_dataset = (
            _map_batches(self.ray_dataset, calc_sort_key)
            .sort(SORT_KEY, descending=not ascending)
            .drop_columns([SORT_KEY])
        )
        return RayDataFrame(sorted_dataset)

    def add_metadata(self, func: Callable[[Schema], dict[str, Any]]) -> "DataFrame":
        def add_metadata_to_schema(schema: Schema) -> Schema:
            schema.metadata.update(func(schema))
            return schema

        return self.map(add_metadata_to_schema)

    def count(self) -> int:
        return int(self.ray_dataset.count())

    def limit(self, num_rows: int) -> "DataFrame":
        limited_dataset = self.ray_dataset.limit(num_rows)
        return RayDataFrame(limited_dataset)

    def shuffle(self) -> "DataFrame":
        shuffled_dataset = self.ray_dataset.random_shuffle()
        return RayDataFrame(shuffled_dataset)

    def repartition(self, num_partitions: int) -> "DataFrame":
        repartitioned_dataset = self.ray_dataset.repartition(num_partitions)
        return RayDataFrame(repartitioned_dataset)

    def split(self, n: int) -> list["DataFrame"]:
        return [RayDataFrame(dataset) for dataset in self.ray_dataset.split(n)]

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        def sum_batch(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{SUM_KEY: sum(func(row_to_schema(row)) for row in rows)}]

        partial_sums = _map_batches(self.ray_dataset, sum_batch).take_all()
        sum_result: Union[int, float] = sum(row[SUM_KEY] for row in partial_sums)
        return sum_result

    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        if not isinstance(others, list):
            others = [others]

        other_datasets = []
        for other in others:
            assert isinstance(other, RayDataFrame)
            other_datasets.append(other.ray_dataset)

        return RayDataFrame(self.ray_dataset.union(other_datasets))

    def cache(self) -> "DataFrame":
        return RayDataFrame(self.ray_dataset.materialize())

    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        rows = [schema_to_row(schema) for schema in schemas]
        dataset = ray.data.from_items(rows)
        return RayDataFrame(dataset)

    def collect(self) -> list[Schema]:
        return [row_to_schema(row) for row in self.ray_dataset.take_all()]

    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        data_handler = data_handler or DefaultDataHandler()

        def load_json_line(row: dict[str, Any]) -> dict[str, Any]:
            data_dict = json.loads(row["text"])
            schema = data_handler.from_dict(data_dict)
            return schema_to_row(schema)

        dataset = ray.data.read_text(path, file_extensions=["json", "jsonl"]).map(
            load_json_line
        )
        return RayDataFrame(dataset)

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        data_handler = data_handler or DefaultDataHandler()
        self.ray_dataset.write_datasink(SchemaRowDatasink(path, data_handler))
