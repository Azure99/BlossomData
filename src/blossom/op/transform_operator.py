from typing import Callable, Optional

from blossom.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class TransformOperator(Operator):
    """
    Operator that transforms batches of items in the dataset.

    Unlike MapOperator which processes items one by one, TransformOperator
    processes batches of items together, allowing for operations that depend
    on multiple items at once.
    """

    def __init__(
        self,
        transform_func: Optional[Callable[[list[Schema]], list[Schema]]] = None,
    ):
        """
        Initialize a TransformOperator.

        Args:
            transform_func: Function that transforms a list of schemas into another list
        """
        super().__init__()
        self.transform_func = transform_func

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Process the dataframe by applying the transform function to batches of items.

        Args:
            dataframe: Input dataframe

        Returns:
            Transformed dataframe
        """
        return dataframe.transform(self.process_items)

    def process_items(self, items: list[Schema]) -> list[Schema]:
        """
        Process a batch of items using the transform function.

        Args:
            items: List of input schemas

        Returns:
            List of transformed schemas

        Raises:
            NotImplementedError: If transform_func is None and this method is not overridden
        """
        if self.transform_func is None:
            raise NotImplementedError("transform function not implemented")
        return self.transform_func(items)


def transform_operator() -> Callable[..., TransformOperator]:
    """
    Decorator to create a TransformOperator from a function.

    Returns:
        A decorator that converts a function to a TransformOperator
    """

    def decorator(func: Callable[[list[Schema]], list[Schema]]) -> TransformOperator:
        return TransformOperator(transform_func=func)

    return decorator


def context_transform_operator() -> Callable[..., TransformOperator]:
    """
    Decorator to create a TransformOperator from a function that requires a context.

    The decorated function should take a context and a list of schemas as arguments.

    Returns:
        A decorator that converts a context-aware function to a TransformOperator
    """

    def decorator(
        func: Callable[[Context, list[Schema]], list[Schema]]
    ) -> TransformOperator:
        class WrappedTransformOperator(TransformOperator):
            def process_items(self, data: list[Schema]) -> list[Schema]:
                return func(self.context, data)

        return WrappedTransformOperator()

    return decorator
