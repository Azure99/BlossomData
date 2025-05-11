import json
from typing import Callable, Iterator, Optional, Any, Union

import numpy as np
import pyarrow as pa
import ray
import ray.data
import ray.data.grouped_data
from ray.data.aggregate import AggregateFn
from ray.data.block import BlockAccessor
from ray.data.datasource import BlockBasedFileDatasink

from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame, GroupedDataFrame
from blossom.dataframe.default_data_handler import DefaultDataHandler
from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import (
    FIELD_DATA,
    FIELD_FAILED,
    FIELD_ID,
    FIELD_METADATA,
    FIELD_TYPE,
    Schema,
)

SORT_KEY = "__sort_key__"
GROUP_KEY = "__group_key__"
AGGREGATE_NAME = "__aggregate_name__"


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

    ds: ray.data.Dataset = dataset.map_batches(_map_batch)
    return ds


class SchemaRowDataSink(BlockBasedFileDatasink):
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


class RayAggregateFn(AggregateFn):
    def __init__(self, *aggs: AggregateFunc):
        super().__init__(
            init=lambda k: [schema_to_row(agg.initial_value) for agg in aggs],
            accumulate_row=lambda a, row: [
                schema_to_row(
                    aggs[i].accumulate(row_to_schema(a[i]), row_to_schema(row))
                )
                for i in range(len(aggs))
            ],
            merge=lambda a1, a2: [
                schema_to_row(aggs[i].merge(row_to_schema(a1[i]), row_to_schema(a2[i])))
                for i in range(len(aggs))
            ],
            name=AGGREGATE_NAME,
        )


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

    def aggregate(
        self,
        *aggs: AggregateFunc,
    ) -> Union[Any, dict[str, Any]]:
        result = self.ray_dataset.aggregate(RayAggregateFn(*aggs))
        result_dict = {
            aggs[i].name: aggs[i].finalize(row_to_schema(result[AGGREGATE_NAME][i]))
            for i in range(len(aggs))
        }
        return result_dict if len(aggs) > 1 else result_dict[aggs[0].name]

    def group_by(
        self, func: Callable[[Schema], Any], name: str = "group"
    ) -> "GroupedRayDataFrame":
        def calc_group_key(batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
            return [{**row, GROUP_KEY: func(row_to_schema(row))} for row in batch]

        grouped_dataset = _map_batches(self.ray_dataset, calc_group_key).groupby(
            GROUP_KEY
        )
        return GroupedRayDataFrame(name, grouped_dataset)

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
        self.ray_dataset.write_datasink(SchemaRowDataSink(path, data_handler))


class GroupedRayDataFrame(GroupedDataFrame):
    def __init__(self, name: str, grouped_data: ray.data.grouped_data.GroupedData):
        super().__init__(name)
        self.grouped_data = grouped_data

    def aggregate(self, *aggs: AggregateFunc) -> DataFrame:
        aggregated_ds = self.grouped_data.aggregate(RayAggregateFn(*aggs))

        def map_to_schema(row: dict[str, Any]) -> dict[str, Any]:
            result_row = RowSchema(data={self.name: row[GROUP_KEY]})

            for i in range(len(aggs)):
                result = row[AGGREGATE_NAME][i]
                result_row[aggs[i].name] = aggs[i].finalize(row_to_schema(result))
            return schema_to_row(result_row)

        return RayDataFrame(aggregated_ds.map(map_to_schema))
