from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class FailedItemFilter(Operator):
    def __init__(
        self,
        reverse: bool = False,
    ):
        super().__init__()
        self.reverse = reverse

    def process(self, data: list[Schema]) -> list[Schema]:
        results = list(map(self.process_item, data))
        return [item for item, passed in zip(data, results) if passed ^ self.reverse]

    @staticmethod
    def process_item(item: Schema) -> bool:
        return not item.failed
