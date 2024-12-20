from typing import Callable, Optional
from blossom.context import Context

from blossom.op.base_operator import BaseOperator
from blossom.schema.base_schema import BaseSchema


class TransformOperator(BaseOperator):
    def __init__(
        self,
        transform_func: Optional[Callable[[list[BaseSchema]], list[BaseSchema]]] = None,
    ):
        super().__init__()
        self.transform_func = transform_func

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        if self.transform_func is None:
            raise NotImplementedError("transform function not implemented")
        return self.transform_func(data)


def transform_operator() -> Callable[..., TransformOperator]:
    def decorator(
        func: Callable[[list[BaseSchema]], list[BaseSchema]]
    ) -> TransformOperator:
        return TransformOperator(transform_func=func)

    return decorator


def context_transform_operator() -> Callable[..., TransformOperator]:
    def decorator(
        func: Callable[[Context, list[BaseSchema]], list[BaseSchema]]
    ) -> TransformOperator:
        class WrappedTransformOperator(TransformOperator):
            def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
                return func(self.context, data)

        return WrappedTransformOperator()

    return decorator
