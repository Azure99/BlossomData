from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class MapOperator(Operator):
    def __init__(
        self,
        map_func: Optional[Callable[[Schema], Schema]] = None,
        parallel: int = 1,
    ):
        super().__init__()
        self.map_func = map_func
        self.parallel = parallel

    def process(self, dataframe: DataFrame) -> DataFrame:
        def batch_map(data: list[Schema]) -> list[Schema]:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                return list(executor.map(self.process_skip_failed, data))

        if self.parallel > 1:
            return dataframe.transform(batch_map)
        return dataframe.map(self.process_skip_failed)

    def process_skip_failed(self, item: Schema) -> Schema:
        if item.failed:
            return item
        return self.process_item(item)

    def process_item(self, item: Schema) -> Schema:
        if self.map_func is None:
            raise NotImplementedError("map function not implemented")
        return self.map_func(item)


def map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    def decorator(func: Callable[[Schema], Schema]) -> MapOperator:
        return MapOperator(map_func=func, parallel=parallel)

    return decorator


def context_map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    def decorator(func: Callable[[Context, Schema], Schema]) -> MapOperator:
        class WrappedMapOperator(MapOperator):
            def __init__(self, map_parallel: int):
                super().__init__(parallel=map_parallel)

            def process_item(self, item: Schema) -> Schema:
                return func(self.context, item)

        return WrappedMapOperator(map_parallel=parallel)

    return decorator
