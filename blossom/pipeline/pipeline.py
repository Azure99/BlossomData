from abc import ABC, abstractmethod
from typing import Callable, Optional

from blossom.conf import Config
from blossom.conf.config import load_config
from blossom.context.context import Context
from blossom.op.operator import Operator
from blossom.op.filter_operator import FilterOperator
from blossom.op.map_operator import MapOperator
from blossom.op.transform_operator import TransformOperator
from blossom.schema.schema import Schema


class Pipeline(ABC):
    def __init__(self, config: Optional[Config] = None):
        self.config: Config = load_config() if config is None else config
        self.context = Context(self.config)
        self.operators: list[Operator] = []

    def add_operator(self, operator: Operator) -> "Pipeline":
        operator.init_context(self.context)
        self.operators.append(operator)
        return self

    def add_operators(self, *operators: Operator) -> "Pipeline":
        for operator in operators:
            self.add_operator(operator)
        return self

    @abstractmethod
    def execute(self, data: list[Schema]) -> list[Schema]:
        pass

    def filter(
        self, filter_func: Callable[[Schema], bool], parallel: int = 1
    ) -> "Pipeline":
        return self.add_operator(
            FilterOperator(filter_func=filter_func, parallel=parallel)
        )

    def map(
        self, map_func: Callable[[Schema], Schema], parallel: int = 1
    ) -> "Pipeline":
        return self.add_operator(MapOperator(map_func=map_func, parallel=parallel))

    def transform(
        self, transform_func: Callable[[list[Schema]], list[Schema]]
    ) -> "Pipeline":
        return self.add_operator(TransformOperator(transform_func))
