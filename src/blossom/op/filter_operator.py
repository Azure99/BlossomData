from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class FilterOperator(Operator):
    """
    Operator that filters items in the dataset based on a predicate function.

    This operator keeps or removes items based on the result of the predicate function.
    It can optionally process items in parallel using a thread pool.
    """

    def __init__(
        self,
        filter_func: Optional[Callable[[Schema], bool]] = None,
        reverse: bool = False,
        parallel: int = 1,
    ):
        """
        Initialize a FilterOperator.

        Args:
            filter_func: Predicate function that returns True for items to keep
            reverse: If True, keep items where the predicate returns False (default: False)
            parallel: Number of parallel threads to use (default: 1)
        """
        super().__init__()
        self.filter_func = filter_func
        self.reverse = reverse
        self.parallel = parallel

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Process the dataframe by applying the filter function to each item.

        If parallel > 1, uses a thread pool to process items in parallel.

        Args:
            dataframe: Input dataframe

        Returns:
            Filtered dataframe
        """

        def batch_filter(data: list[Schema]) -> list[Schema]:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                results = list(executor.map(self.process_skip_failed, data))
                return [
                    item for item, passed in zip(data, results) if passed ^ self.reverse
                ]

        if self.parallel > 1:
            return dataframe.transform(batch_filter)
        return dataframe.filter(
            lambda item: self.process_skip_failed(item) ^ self.reverse
        )

    def process_skip_failed(self, item: Schema) -> bool:
        """
        Process an item, automatically passing if it has already failed.

        Args:
            item: Input schema

        Returns:
            True if the item should be kept (before considering reverse),
            False otherwise
        """
        if item.failed:
            return True
        return self.process_item(item)

    def process_item(self, item: Schema) -> bool:
        """
        Process a single item using the filter function.

        Args:
            item: Input schema

        Returns:
            True if the item should be kept (before considering reverse),
            False otherwise

        Raises:
            NotImplementedError: If filter_func is None and this method is not overridden
        """
        if self.filter_func is None:
            raise NotImplementedError("filter function not implemented")
        return self.filter_func(item)


def filter_operator(
    reverse: bool = False, parallel: int = 1
) -> Callable[..., FilterOperator]:
    """
    Decorator to create a FilterOperator from a function.

    Args:
        reverse: If True, keep items where the predicate returns False (default: False)
        parallel: Number of parallel threads to use (default: 1)

    Returns:
        A decorator that converts a function to a FilterOperator
    """

    def decorator(func: Callable[[Schema], bool]) -> FilterOperator:
        return FilterOperator(filter_func=func, reverse=reverse, parallel=parallel)

    return decorator


def context_filter_operator(parallel: int = 1) -> Callable[..., FilterOperator]:
    """
    Decorator to create a FilterOperator from a function that requires a context.

    The decorated function should take a context and a schema as arguments.

    Args:
        parallel: Number of parallel threads to use (default: 1)

    Returns:
        A decorator that converts a context-aware function to a FilterOperator
    """

    def decorator(func: Callable[[Context, Schema], bool]) -> FilterOperator:
        class WrappedFilterOperator(FilterOperator):
            def __init__(self, filter_parallel: int):
                super().__init__(parallel=filter_parallel)

            def process_item(self, item: Schema) -> bool:
                return func(self.context, item)

        return WrappedFilterOperator(filter_parallel=parallel)

    return decorator
