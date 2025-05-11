from abc import ABC, abstractmethod
from typing import Any, Callable, Optional, Union

from blossom.dataframe.aggregate import (
    AggregateFunc,
    Count,
    Max,
    Mean,
    Min,
    StdDev,
    Sum,
    Unique,
    Variance,
)
from blossom.dataframe.data_handler import DataHandler
from blossom.schema.schema import Schema


class DataFrame(ABC):
    """
    Abstract base class for dataframes.

    A DataFrame represents a distributed collection of data items that can be
    processed in parallel. This class defines the interface for all dataframe
    implementations, providing methods for data transformation, filtering,
    aggregation, and I/O operations.
    """

    @abstractmethod
    def map(self, func: Callable[[Schema], Schema]) -> "DataFrame":
        """
        Apply a function to each item in the dataframe.

        Args:
            func: Function that takes a schema and returns a transformed schema

        Returns:
            A new dataframe with the transformed items
        """
        pass

    @abstractmethod
    def filter(self, func: Callable[[Schema], bool]) -> "DataFrame":
        """
        Filter items in the dataframe based on a predicate function.

        Args:
            func: Predicate function that returns True for items to keep

        Returns:
            A new dataframe with only the items that satisfy the predicate
        """
        pass

    @abstractmethod
    def transform(self, func: Callable[[list[Schema]], list[Schema]]) -> "DataFrame":
        """
        Apply a transformation to batches of items in the dataframe.

        Args:
            func: Function that takes a list of schemas and returns a transformed list

        Returns:
            A new dataframe with the transformed items
        """
        pass

    @abstractmethod
    def sort(
        self, func: Callable[[Schema], Any], ascending: bool = True
    ) -> "DataFrame":
        """
        Sort the dataframe based on a key function.

        Args:
            func: Function that extracts the sort key from each item
            ascending: Whether to sort in ascending order (default: True)

        Returns:
            A new dataframe with sorted items
        """
        pass

    @abstractmethod
    def collect(self) -> list[Schema]:
        """
        Collect all items in the dataframe into a list.

        Returns:
            List of all items in the dataframe
        """
        pass

    @abstractmethod
    def limit(self, num_rows: int) -> "DataFrame":
        """
        Limit the dataframe to a specified number of items.

        Args:
            num_rows: Maximum number of items to keep

        Returns:
            A new dataframe with at most num_rows items
        """
        pass

    @abstractmethod
    def shuffle(self) -> "DataFrame":
        """
        Randomly shuffle the items in the dataframe.

        Returns:
            A new dataframe with shuffled items
        """
        pass

    @abstractmethod
    def repartition(self, num_partitions: int) -> "DataFrame":
        """
        Repartition the dataframe into a specified number of partitions.

        Args:
            num_partitions: Number of partitions

        Returns:
            A new dataframe with the specified number of partitions
        """
        pass

    @abstractmethod
    def split(self, n: int) -> list["DataFrame"]:
        """
        Split the dataframe into n equal parts.

        Args:
            n: Number of parts to split into

        Returns:
            List of n dataframes
        """
        pass

    @abstractmethod
    def aggregate(
        self,
        *aggs: AggregateFunc,
    ) -> Union[Any, dict[str, Any]]:
        """
        Aggregate the dataframe using one or more aggregate functions.

        Args:
            *aggs: Aggregate functions to apply

        Returns:
            Single aggregation result or dictionary of named results
        """
        pass

    @abstractmethod
    def group_by(
        self, func: Callable[[Schema], Any], name: str = "group"
    ) -> "GroupedDataFrame":
        """
        Group the dataframe by a key function.

        Args:
            func: Function that extracts the grouping key from each item
            name: Name of the grouping key in the result (default: "group")

        Returns:
            A grouped dataframe
        """
        pass

    @abstractmethod
    def union(self, others: Union["DataFrame", list["DataFrame"]]) -> "DataFrame":
        """
        Union this dataframe with one or more other dataframes.

        Args:
            others: Dataframe or list of dataframes to union with

        Returns:
            A new dataframe containing all items from all dataframes
        """
        pass

    @abstractmethod
    def cache(self) -> "DataFrame":
        """
        Cache the dataframe in memory.

        Returns:
            The cached dataframe
        """
        pass

    @abstractmethod
    def from_list(self, schemas: list[Schema]) -> "DataFrame":
        """
        Create a dataframe from a list of schemas.

        Args:
            schemas: List of schemas

        Returns:
            A new dataframe containing the schemas
        """
        pass

    @abstractmethod
    def read_json(
        self, path: str, data_handler: Optional[DataHandler] = None
    ) -> "DataFrame":
        """
        Read a dataframe from a JSON file.

        Args:
            path: Path to the JSON file
            data_handler: Optional data handler for custom deserialization

        Returns:
            A new dataframe containing the items from the JSON file
        """
        pass

    @abstractmethod
    def write_json(self, path: str, data_handler: Optional[DataHandler] = None) -> None:
        """
        Write the dataframe to a JSON file.

        Args:
            path: Path to the JSON file
            data_handler: Optional data handler for custom serialization
        """
        pass

    def add_metadata(self, func: Callable[[Schema], dict[str, Any]]) -> "DataFrame":
        """
        Add metadata to each item in the dataframe.

        Args:
            func: Function that takes a schema and returns metadata to add

        Returns:
            A new dataframe with added metadata
        """

        def add_metadata_to_schema(schema: Schema) -> Schema:
            schema.metadata.update(func(schema))
            return schema

        return self.map(add_metadata_to_schema)

    def drop_metadata(self, keys: list[str]) -> "DataFrame":
        """
        Drop specified metadata keys from each item in the dataframe.

        Args:
            keys: List of metadata keys to drop

        Returns:
            A new dataframe with the specified metadata keys dropped
        """

        def drop_metadata_from_schema(schema: Schema) -> Schema:
            for key in keys:
                schema.metadata.pop(key, None)
            return schema

        return self.map(drop_metadata_from_schema)

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        """
        Calculate the sum of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Sum of the extracted values
        """
        result = self.aggregate(Sum(func))
        assert isinstance(result, (int, float))
        return result

    def mean(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        """
        Calculate the mean of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Mean of the extracted values
        """
        result = self.aggregate(Mean(func))
        assert isinstance(result, (int, float))
        return result

    def count(self) -> int:
        """
        Count the number of items in the dataframe.

        Returns:
            Number of items
        """
        result = self.aggregate(Count())
        assert isinstance(result, int)
        return result

    def min(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        """
        Find the minimum value of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Minimum of the extracted values
        """
        result = self.aggregate(Min(func))
        assert isinstance(result, (int, float))
        return result

    def max(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        """
        Find the maximum value of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Maximum of the extracted values
        """
        result = self.aggregate(Max(func))
        assert isinstance(result, (int, float))
        return result

    def variance(
        self, func: Callable[[Schema], Union[int, float]]
    ) -> Union[int, float]:
        """
        Calculate the variance of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Variance of the extracted values
        """
        result = self.aggregate(Variance(func))
        assert isinstance(result, (int, float))
        return result

    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> Union[int, float]:
        """
        Calculate the standard deviation of a numeric function applied to each item.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            Standard deviation of the extracted values
        """
        result = self.aggregate(StdDev(func))
        assert isinstance(result, (int, float))
        return result

    def unique(self, func: Callable[[Schema], set[Any]]) -> list[Any]:
        """
        Find unique values of a function applied to each item.

        Args:
            func: Function that extracts values from each item

        Returns:
            List of unique values
        """
        result = self.aggregate(Unique(func))
        assert isinstance(result, list)
        return result


class GroupedDataFrame(ABC):
    """
    Abstract base class for grouped dataframes.

    A GroupedDataFrame represents a dataframe that has been grouped by a key function.
    This class defines the interface for grouped dataframe operations, providing
    methods for aggregation and other group-wise operations.
    """

    def __init__(self, name: str):
        """
        Initialize a grouped dataframe with a group name.

        Args:
            name: Name of the grouping key in the result
        """
        self.name = name

    @abstractmethod
    def aggregate(self, *aggs: AggregateFunc) -> DataFrame:
        """
        Aggregate the grouped dataframe using one or more aggregate functions.

        Args:
            *aggs: Aggregate functions to apply to each group

        Returns:
            A dataframe with one row per group containing the aggregation results
        """
        pass

    def sum(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Calculate the sum of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the sum
        """
        return self.aggregate(Sum(func))

    def mean(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Calculate the mean of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the mean
        """
        return self.aggregate(Mean(func))

    def count(self) -> DataFrame:
        """
        Count the number of items in each group.

        Returns:
            A dataframe with one row per group containing the count
        """
        return self.aggregate(Count())

    def min(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Find the minimum value of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the minimum value
        """
        return self.aggregate(Min(func))

    def max(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Find the maximum value of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the maximum value
        """
        return self.aggregate(Max(func))

    def variance(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Calculate the variance of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the variance
        """
        return self.aggregate(Variance(func))

    def stddev(self, func: Callable[[Schema], Union[int, float]]) -> DataFrame:
        """
        Calculate the standard deviation of a numeric function for each group.

        Args:
            func: Function that extracts a numeric value from each item

        Returns:
            A dataframe with one row per group containing the standard deviation
        """
        return self.aggregate(StdDev(func))

    def unique(self, func: Callable[[Schema], set[Any]]) -> DataFrame:
        """
        Find unique values of a function for each group.

        Args:
            func: Function that extracts values from each item

        Returns:
            A dataframe with one row per group containing the unique values
        """
        return self.aggregate(Unique(func))
