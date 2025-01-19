from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class FailedItemFilter(BaseOperator):
    def __init__(
        self,
        reverse: bool = False,
    ):
        self.reverse = reverse

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        results = list(map(self.process_item, data))
        return [item for item, passed in zip(data, results) if passed ^ self.reverse]

    def process_item(self, item: BaseSchema) -> bool:
        return not item.failed
