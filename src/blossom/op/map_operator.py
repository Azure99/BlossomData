from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional

from blossom.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.op.operator import Operator
from blossom.schema.schema import Schema


class MapOperator(Operator):
    """
    Operator that applies a mapping function to each item in the dataset.

    This operator transforms each item independently using the provided mapping function.
    It can optionally process items in parallel using a thread pool.
    """

    def __init__(
        self,
        map_func: Optional[Callable[[Schema], Schema]] = None,
        parallel: int = 1,
    ):
        """
        Initialize a MapOperator.

        Args:
            map_func: Function that maps an input schema to an output schema
            parallel: Number of parallel threads to use (default: 1)
        """
        super().__init__()
        self.map_func = map_func
        self.parallel = parallel

    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Process the dataframe by applying the mapping function to each item.

        If parallel > 1, uses a thread pool to process items in parallel.

        Args:
            dataframe: Input dataframe

        Returns:
            Processed dataframe
        """

        def batch_map(data: list[Schema]) -> list[Schema]:
            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                return list(executor.map(self.process_skip_failed, data))

        if self.parallel > 1:
            return dataframe.transform(batch_map)
        return dataframe.map(self.process_skip_failed)

    def process_skip_failed(self, item: Schema) -> Schema:
        """
        Process an item, skipping if it has already failed.

        Args:
            item: Input schema

        Returns:
            Processed schema or original schema if already failed
        """
        if item.failed:
            return item
        return self.process_item(item)

    def process_item(self, item: Schema) -> Schema:
        """
        Process a single item using the mapping function.

        Args:
            item: Input schema

        Returns:
            Processed schema

        Raises:
            NotImplementedError: If map_func is None and this method is not overridden
        """
        if self.map_func is None:
            raise NotImplementedError("map function not implemented")
        return self.map_func(item)


def map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    """
    Decorator to create a MapOperator from a function.

    Args:
        parallel: Number of parallel threads to use (default: 1)

    Returns:
        A decorator that converts a function to a MapOperator
    """

    def decorator(func: Callable[[Schema], Schema]) -> MapOperator:
        return MapOperator(map_func=func, parallel=parallel)

    return decorator


def context_map_operator(parallel: int = 1) -> Callable[..., MapOperator]:
    """
    Decorator to create a MapOperator from a function that requires a context.

    The decorated function should take a context and a schema as arguments.

    Args:
        parallel: Number of parallel threads to use (default: 1)

    Returns:
        A decorator that converts a context-aware function to a MapOperator
    """

    def decorator(func: Callable[[Context, Schema], Schema]) -> MapOperator:
        class WrappedMapOperator(MapOperator):
            def __init__(self, map_parallel: int):
                super().__init__(parallel=map_parallel)

            def process_item(self, item: Schema) -> Schema:
                return func(self.context, item)

        return WrappedMapOperator(map_parallel=parallel)

    return decorator
