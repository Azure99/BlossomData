from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

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
                return list(executor.map(self.process_item, data))

        return list(map(self.process_item, data))

    def process_item(self, item: BaseSchema) -> BaseSchema:
        if self.map_func is None:
            raise NotImplementedError("map function not implemented")
        return self.map_func(item)
