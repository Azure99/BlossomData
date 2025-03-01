from typing import Callable, Optional

from blossom.context import Context
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class TransformOperator(Operator):
    def __init__(
        self,
        transform_func: Optional[Callable[[list[Schema]], list[Schema]]] = None,
    ):
        super().__init__()
        self.transform_func = transform_func

    def process(self, data: list[Schema]) -> list[Schema]:
        if self.transform_func is None:
            raise NotImplementedError("transform function not implemented")
        return self.transform_func(data)


def transform_operator() -> Callable[..., TransformOperator]:
    def decorator(func: Callable[[list[Schema]], list[Schema]]) -> TransformOperator:
        return TransformOperator(transform_func=func)

    return decorator


def context_transform_operator() -> Callable[..., TransformOperator]:
    def decorator(
        func: Callable[[Context, list[Schema]], list[Schema]]
    ) -> TransformOperator:
        class WrappedTransformOperator(TransformOperator):
            def process(self, data: list[Schema]) -> list[Schema]:
                return func(self.context, data)

        return WrappedTransformOperator()

    return decorator
