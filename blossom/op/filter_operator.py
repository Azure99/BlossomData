from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.context import Context
from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class FilterOperator(BaseOperator):
    def __init__(
        self,
        filter_func: Optional[Callable[[BaseSchema], bool]] = None,
        reverse: bool = False,
        parallel: int = 1,
    ):
        super().__init__()
        self.filter_func = filter_func
        self.reverse = reverse
        self.parallel = parallel

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        if self.parallel > 1:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                results = list(executor.map(self.process_skip_failed, data))
        else:
            results = list(map(self.process_skip_failed, data))
        return [item for item, passed in zip(data, results) if passed ^ self.reverse]

    def process_skip_failed(self, item: BaseSchema) -> bool:
        if item.failed:
            return True
        return self.process_item(item)

    def process_item(self, item: BaseSchema) -> bool:
        if self.filter_func is None:
            raise NotImplementedError("filter function not implemented")
        return self.filter_func(item)


def filter_operator(
    reverse: bool = False, parallel: int = 1
) -> Callable[..., FilterOperator]:
    def decorator(func: Callable[[BaseSchema], bool]) -> FilterOperator:
        return FilterOperator(filter_func=func, reverse=reverse, parallel=parallel)

    return decorator


def context_filter_operator(parallel: int = 1) -> Callable[..., FilterOperator]:
    def decorator(func: Callable[[Context, BaseSchema], bool]) -> FilterOperator:
        class WrappedFilterOperator(FilterOperator):
            def __init__(self, filter_parallel: int):
                super().__init__(parallel=filter_parallel)

            def process_item(self, item: BaseSchema) -> bool:
                return func(self.context, item)

        return WrappedFilterOperator(filter_parallel=parallel)

    return decorator
