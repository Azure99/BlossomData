from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class FilterOperator(BaseOperator):
    def __init__(
        self,
        filter_func: Optional[Callable[[BaseSchema], bool]] = None,
        parallel: int = 1,
    ):
        super().__init__()
        self.filter_func = filter_func
        self.parallel = parallel

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        if self.parallel > 1:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                results = list(executor.map(self.process_item, data))
        else:
            results = list(map(self.process_item, data))
        return [item for item, passed in zip(data, results) if passed]

    def process_item(self, item: BaseSchema) -> bool:
        if self.filter_func is None:
            raise NotImplementedError("filter function not implemented")
        return self.filter_func(item)
