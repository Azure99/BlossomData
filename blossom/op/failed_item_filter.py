from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class FailedItemFilter(BaseOperator):
    def __init__(
        self,
        reverse: bool = False,
    ):
        super().__init__()
        self.reverse = reverse

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        results = list(map(self.process_item, data))
        return [item for item, passed in zip(data, results) if passed ^ self.reverse]

    @staticmethod
    def process_item(item: BaseSchema) -> bool:
        return not item.failed
