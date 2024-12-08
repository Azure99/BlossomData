from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema


class FailedItemFilter(FilterOperator):
    def __init__(
        self,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)

    def process_item(self, item: BaseSchema) -> bool:
        return not item.failed
