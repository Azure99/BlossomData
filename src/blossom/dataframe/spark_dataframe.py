import json
import random
from typing import Callable, Optional, Any, Union
from collections.abc import Iterable

from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from blossom.dataframe.aggregate import AggregateFunc
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame, GroupedDataFrame
from blossom.dataframe.default_data_handler import DefaultDataHandler
from blossom.schema.pair_schema import PairSchema
from blossom.schema.schema import Schema


GROUP_KEY = "__group_key__"


class SparkDataFrame(DataFrame):
    def __init__(
        self,
        spark_rdd: Optional[RDD[dict[str, Any]]] = None,
        spark_session: Optional[SparkSession] = None,
    ):
        self.spark_session = spark_session or SparkSession.builder.getOrCreate()
        self.spark_rdd = spark_rdd or self.spark_session.sparkContext.emptyRDD()

    def map(self, func: Callable[[Schema], Schema]) -> "SparkDataFrame":
        def map_row(row_dict: dict[str, Any]) -> dict[str, Any]:
            schema = Schema.from_dict(row_dict)
            return func(schema).to_dict()

        mapped_rdd = self.spark_rdd.map(map_row)
        return SparkDataFrame(mapped_rdd, self.spark_session)

    def filter(self, func: Callable[[Schema], bool]) -> "SparkDataFrame":
        def filter_row(row_dict: dict[str, Any]) -> bool:
            schema = Schema.from_dict(row_dict)
            return func(schema)

        filtered_rdd = self.spark_rdd.filter(filter_row)
        return SparkDataFrame(filtered_rdd, self.spark_session)

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        def transform_partition(
            iterator: Iterable[dict[str, Any]]
        ) -> Iterable[dict[str, Any]]:
            schemas = [Schema.from_dict(row_dict) for row_dict in iterator]
            transformed_schemas = func(schemas)
            return iter([schema.to_dict() for schema in transformed_schemas])

        transformed_rdd = self.spark_rdd.mapPartitions(transform_partition)
        return SparkDataFrame(transformed_rdd, self.spark_session)

    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        sorted_rdd = self.spark_rdd.sortBy(
            lambda x: func(Schema.from_dict(x)), ascending=ascending
        )
        return SparkDataFrame(sorted_rdd, self.spark_session)

    def limit(self, num_rows: int) -> "DataFrame":
        limited_rdd = (
            self.spark_rdd.zipWithIndex()
            .filter(lambda x: x[1] < num_rows)
            .map(lambda x: x[0])
        )
        return SparkDataFrame(limited_rdd, self.spark_session)

    def shuffle(self) -> "DataFrame":
        shuffled_rdd = (
            self.spark_rdd.map(lambda x: (random.random(), x))
            .sortBy(lambda x: x[0])  # type: ignore
            .map(lambda x: x[1])
        )
        return SparkDataFrame(shuffled_rdd, self.spark_session)

    def repartition(self, num_partitions: int) -> "DataFrame":
        return SparkDataFrame(
            self.spark_rdd.repartition(num_partitions), self.spark_session
        )

    def split(self, n: int) -> list["DataFrame"]:
        rdd = self.spark_rdd.zipWithIndex().cache()
        total = rdd.count()
        chunk_size = total // n
        remainder = total % n
        dataframes: list[DataFrame] = []
        start = 0
        for i in range(n):
            end = start + chunk_size + (1 if i < remainder else 0)

            def make_filter(
                s: int, e: int
            ) -> Callable[[tuple[dict[str, Any], int]], bool]:
                return lambda x: x[1] >= s and x[1] < e

            chunk_rdd = rdd.filter(make_filter(start, end)).map(lambda x: x[0])
            dataframes.append(SparkDataFrame(chunk_rdd, self.spark_session))
            start = end
        return dataframes

    def aggregate(
        self,
        aggregate_func: AggregateFunc,
    ) -> Any:
        result = self.spark_rdd.aggregate(
            zeroValue=aggregate_func.initial_value.to_dict(),
            seqOp=lambda x, y: aggregate_func.accumulate(
                Schema.from_dict(x), Schema.from_dict(y)
            ).to_dict(),
            combOp=lambda x, y: aggregate_func.merge(
                Schema.from_dict(x), Schema.from_dict(y)
            ).to_dict(),
        )
        return aggregate_func.finalize(Schema.from_dict(result))

    def group_by(self, func: Callable[[Schema], Any]) -> "GroupedDataFrame":
        def calc_group_key(row_dict: dict[str, Any]) -> Any:
            schema = Schema.from_dict(row_dict)
            return func(schema)

        rdd_with_group_key = self.spark_rdd.map(lambda x: (calc_group_key(x), x))
        return GroupedSparkDataFrame(rdd_with_group_key, self.spark_session)

    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        if not isinstance(others, list):
            others = [others]

        unioned_rdd = self.spark_rdd
        for other in others:
            assert isinstance(other, SparkDataFrame)
            unioned_rdd = unioned_rdd.union(other.spark_rdd)

        return SparkDataFrame(unioned_rdd, self.spark_session)

    def cache(self) -> "DataFrame":
        return SparkDataFrame(self.spark_rdd.cache(), self.spark_session)

    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        row_dicts = [schema.to_dict() for schema in schemas]
        return SparkDataFrame(
            self.spark_session.sparkContext.parallelize(row_dicts), self.spark_session
        )

    def collect(self) -> list[Schema]:
        return [Schema.from_dict(row_dict) for row_dict in self.spark_rdd.collect()]

    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        data_handler = data_handler or DefaultDataHandler()

        def load_json_line(line: str) -> dict[str, Any]:
            data_dict = json.loads(line)
            schema = data_handler.from_dict(data_dict)
            return schema.to_dict()

        rdd = self.spark_session.sparkContext.textFile(path).map(load_json_line)
        return SparkDataFrame(rdd, self.spark_session)

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        data_handler = data_handler or DefaultDataHandler()

        def serialize_row(row_dict: dict[str, Any]) -> str:
            schema = Schema.from_dict(row_dict)
            return json.dumps(data_handler.to_dict(schema), ensure_ascii=False)

        self.spark_rdd.map(serialize_row).saveAsTextFile(path)


class GroupedSparkDataFrame(GroupedDataFrame):
    def __init__(
        self,
        rdd_with_group_key: RDD[tuple[Any, dict[str, Any]]],
        spark_session: SparkSession,
    ):
        self.rdd_with_group_key = rdd_with_group_key
        self.spark_session = spark_session

    def aggregate(self, aggregate_func: AggregateFunc) -> DataFrame:
        aggregated_rdd = self.rdd_with_group_key.aggregateByKey(
            zeroValue=aggregate_func.initial_value.to_dict(),
            seqFunc=lambda x, y: aggregate_func.accumulate(
                Schema.from_dict(x), Schema.from_dict(y)
            ).to_dict(),
            combFunc=lambda x, y: aggregate_func.merge(
                Schema.from_dict(x), Schema.from_dict(y)
            ).to_dict(),
        )

        def map_to_schema(x: tuple[Any, dict[str, Any]]) -> dict[str, Any]:
            key = x[0]
            value = aggregate_func.finalize(Schema.from_dict(x[1]))
            return PairSchema(key=key, value=value).to_dict()

        return SparkDataFrame(aggregated_rdd.map(map_to_schema), self.spark_session)
