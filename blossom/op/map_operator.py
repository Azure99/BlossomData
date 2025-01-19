from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.context import Context
from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class MapOperator(BaseOperator):
    def __init__(
        self,
        map_func: Optional[Callable[[BaseSchema], BaseSchema]] = None,
        parallel: int = 1,
    ):
        super().__init__()
        self.map_func = map_func
        self.parallel = parallel

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        if self.parallel > 1:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                return list(executor.map(self.process_skip_failed, data))

        return list(map(self.process_skip_failed, data))

    def process_skip_failed(self, item: BaseSchema) -> BaseSchema:
        if item.failed:
            return item
        return self.process_item(item)

    def process_item(self, item: BaseSchema) -> BaseSchema:
        if self.map_func is None:
            raise NotImplementedError("map function not implemented")
        return self.map_func(item)


def map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    def decorator(func: Callable[[BaseSchema], BaseSchema]) -> MapOperator:
        return MapOperator(map_func=func, parallel=parallel)

    return decorator


def context_map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    def decorator(func: Callable[[Context, BaseSchema], BaseSchema]) -> MapOperator:
        class WrappedMapOperator(MapOperator):
            def __init__(self, map_parallel: int):
                super().__init__(parallel=map_parallel)

            def process_item(self, item: BaseSchema) -> BaseSchema:
                return func(self.context, item)

        return WrappedMapOperator(map_parallel=parallel)

    return decorator
