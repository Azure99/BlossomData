from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class FailedItemFilter(Operator):
    def __init__(
        self,
        reverse: bool = False,
    ):
        super().__init__()
        self.reverse = reverse

    def process(self, dataframe: DataFrame) -> DataFrame:
        return dataframe.filter(lambda item: self.process_item(item) ^ self.reverse)

    @staticmethod
    def process_item(item: Schema) -> bool:
        return not item.failed
