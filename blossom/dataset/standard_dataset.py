from typing import Callable, Optional
from blossom.context.context import Context
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.dataset.dataset import Dataset
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class StandardDataset(Dataset):
    def __init__(
        self,
        context: Optional[Context] = None,
        dataframe: Optional[DataFrame] = None,
    ):
        super().__init__(context)
        self.dataframe = dataframe or LocalDataFrame()

    def map(self, func: Callable[[Schema], Schema]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.map(func))

    def filter(self, func: Callable[[Schema], bool]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.filter(func))

    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.transform(func))

    def execute(self, operators: list[Operator]) -> "Dataset":
        current_df = self.dataframe
        for operator in operators:
            operator.init_context(self.context)
            current_df = operator.process(current_df)
        return StandardDataset(self.context, current_df)

    def collect(self) -> list[Schema]:
        return self.dataframe.collect()

    def count(self) -> int:
        return self.dataframe.count()

    def limit(self, num_rows: int) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.limit(num_rows))

    def shuffle(self) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.shuffle())

    def repartition(self, num_partitions: int) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.repartition(num_partitions))

    def from_list(self, schemas: list[Schema]) -> "Dataset":
        return StandardDataset(self.context, self.dataframe.from_list(schemas))

    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "Dataset":
        return StandardDataset(
            self.context, self.dataframe.read_json(path, data_handler)
        )

    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        self.dataframe.write_json(path, data_handler)
