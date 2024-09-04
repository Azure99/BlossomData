from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema


class ChatEmptyFilter(FilterOperator):
    def __init__(self):
        super().__init__()
        pass

    def process_item(self, item: BaseSchema) -> bool:
        raise NotImplementedError("Operator not implemented")
