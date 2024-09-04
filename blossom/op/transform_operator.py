from typing import Callable

from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class TransformOperator(BaseOperator):
    def __init__(self, transform_func: Callable[[list[BaseSchema]], list[BaseSchema]]):
        super().__init__()
        self.transform_func = transform_func

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        return self.transform_func(data)
