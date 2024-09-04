from typing import Callable, Optional

from blossom.conf import Config
from blossom.conf.config import load_config
from blossom.context.context import Context
from blossom.op.base_operator import BaseOperator
from blossom.op.filter_operator import FilterOperator
from blossom.op.map_operator import MapOperator
from blossom.op.transform_operator import TransformOperator
from blossom.schema.base_schema import BaseSchema


class BasePipeline:
    def __init__(self, config: Optional[Config] = None):
        self.config: Config = load_config() if config is None else config
        self.context = Context(self.config)
        self.operators: list[BaseOperator] = []

    def add_operator(self, operator: BaseOperator) -> "BasePipeline":
        operator.init_context(self.context)
        self.operators.append(operator)
        return self

    def add_operators(self, *operators: BaseOperator) -> "BasePipeline":
        for operator in operators:
            self.add_operator(operator)
        return self

    def execute(self, data: list[BaseSchema]) -> list[BaseSchema]:
        raise NotImplementedError("Pipeline not implemented")

    def filter(
        self, filter_func: Callable[[BaseSchema], bool], parallel: int = 1
    ) -> "BasePipeline":
        return self.add_operator(
            FilterOperator(filter_func=filter_func, parallel=parallel)
        )

    def map(
        self, map_func: Callable[[BaseSchema], BaseSchema], parallel: int = 1
    ) -> "BasePipeline":
        return self.add_operator(MapOperator(map_func=map_func, parallel=parallel))

    def transform(
        self, transform_func: Callable[[list[BaseSchema]], list[BaseSchema]]
    ) -> "BasePipeline":
        return self.add_operator(TransformOperator(transform_func))
