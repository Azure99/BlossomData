from typing import Callable, Optional

from blossom.context.context import Context
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class Dataset:
    def __init__(
        self,
        dataframe: Optional[DataFrame] = None,
        context: Optional[Context] = None,
    ):
        self.dataframe = dataframe or LocalDataFrame()
        self.context = context or Context()

    def map(self, func: Callable[[Schema], Schema]) -> "Dataset":
        return Dataset(self.dataframe.map(func))

    def filter(self, func: Callable[[Schema], bool]) -> "Dataset":
        return Dataset(self.dataframe.filter(func))

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "Dataset":
        return Dataset(self.dataframe.transform(func))

    def execute(self, operators: list[Operator]) -> "Dataset":
        current_df = self.dataframe
        for operator in operators:
            operator.init_context(self.context)
            current_df = operator.process(current_df)
        return Dataset(current_df)

    def collect(self) -> list[Schema]:
        return self.dataframe.collect()

    def count(self) -> int:
        return self.dataframe.count()

    def limit(self, num_rows: int) -> "Dataset":
        return Dataset(self.dataframe.limit(num_rows))

    def shuffle(self) -> "Dataset":
        return Dataset(self.dataframe.shuffle())

    def repartition(self, num_partitions: int) -> "Dataset":
        return Dataset(self.dataframe.repartition(num_partitions))

    def from_list(self, schemas: list[Schema]) -> "Dataset":
        return Dataset(self.dataframe.from_list(schemas))

    def read_json(self, path: str, data_handler: Optional[DataHandler]) -> "Dataset":
        return Dataset(self.dataframe.read_json(path, data_handler), self.context)

    def write_json(self, path: str, data_handler: Optional[DataHandler]) -> None:
        self.dataframe.write_json(path, data_handler)
