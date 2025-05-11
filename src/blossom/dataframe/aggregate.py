import copy
import math
from typing import Any, Callable, Optional, Union

from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import Schema

SAMPLE_VARIANCE_MIN_DATA_POINTS = 2


class AggregateFunc:
    """
    Base class for aggregation functions.

    An AggregateFunc defines how to accumulate values across items in a dataset,
    merge partial results, and finalize the result. This is the foundation for
    distributed aggregation operations.
    """

    def __init__(
        self,
        initial_value: Schema,
        accumulate: Callable[[Schema, Schema], Schema],
        merge: Callable[[Schema, Schema], Schema],
        finalize: Optional[Callable[[Schema], Any]] = None,
        name: str = "aggregate",
    ):
        """
        Initialize an AggregateFunc.

        Args:
            initial_value: Initial value for the accumulator
            accumulate: Function to accumulate a new item into the accumulator
            merge: Function to merge two accumulators
            finalize: Optional function to transform the final accumulator (default: None)
            name: Name of the aggregation function (default: "aggregate")
        """
        self._initial_value = initial_value
        self._accumulate = accumulate
        self._merge = merge
        self._finalize = finalize
        self._name = name

    @property
    def initial_value(self) -> Schema:
        """
        Get a copy of the initial value.

        Returns:
            Copy of the initial value
        """
        return copy.deepcopy(self._initial_value)

    @property
    def name(self) -> str:
        """
        Get the name of the aggregation function.

        Returns:
            Name of the aggregation function
        """
        return self._name

    def accumulate(self, accumulator: Schema, item: Schema) -> Schema:
        """
        Accumulate a new item into the accumulator.

        Args:
            accumulator: Current accumulator
            item: New item to accumulate

        Returns:
            Updated accumulator
        """
        return self._accumulate(accumulator, item)

    def merge(self, accumulator1: Schema, accumulator2: Schema) -> Schema:
        """
        Merge two accumulators.

        Args:
            accumulator1: First accumulator
            accumulator2: Second accumulator

        Returns:
            Merged accumulator
        """
        return self._merge(accumulator1, accumulator2)

    def finalize(self, accumulator: Schema) -> Any:
        """
        Finalize the accumulator to produce the result.

        Args:
            accumulator: Final accumulator

        Returns:
            Aggregation result
        """
        if self._finalize is None:
            return accumulator
        return self._finalize(accumulator)


class RowAggregateFunc(AggregateFunc):
    """
    Aggregation function that uses RowSchema as the accumulator.

    This is a convenience class for creating aggregation functions that use
    dictionaries as accumulators, wrapped in RowSchema objects.
    """

    def __init__(
        self,
        initial_value: dict[str, Any],
        accumulate: Callable[[dict[str, Any], Schema], dict[str, Any]],
        merge: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
        finalize: Optional[Callable[[dict[str, Any]], Any]] = None,
        name: str = "aggregate",
    ):
        """
        Initialize a RowAggregateFunc.

        Args:
            initial_value: Initial dictionary for the accumulator
            accumulate: Function to accumulate a new item into the dictionary
            merge: Function to merge two dictionaries
            finalize: Optional function to transform the final dictionary (default: None)
            name: Name of the aggregation function (default: "aggregate")
        """

        def _accumulate(x: Schema, y: Schema) -> Schema:
            assert isinstance(x, RowSchema)
            return RowSchema(data=accumulate(x.data, y))

        def _merge(x: Schema, y: Schema) -> Schema:
            assert isinstance(x, RowSchema)
            assert isinstance(y, RowSchema)
            return RowSchema(data=merge(x.data, y.data))

        def _finalize(x: Schema) -> Any:
            assert isinstance(x, RowSchema)
            if finalize is None:
                return x.data
            return finalize(x.data)

        super().__init__(
            RowSchema(data=initial_value),
            _accumulate,
            _merge,
            _finalize,
            name=name,
        )


class Sum(RowAggregateFunc):
    """
    Aggregation function to calculate the sum of values.
    """

    def __init__(self, func: Callable[[Schema], Union[int, float]], name: str = "sum"):
        """
        Initialize a Sum aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "sum")
        """
        super().__init__(
            initial_value={"sum": 0},
            accumulate=lambda x, y: {"sum": x["sum"] + func(y)},
            merge=lambda x, y: {"sum": x["sum"] + y["sum"]},
            finalize=lambda x: x["sum"],
            name=name,
        )


class Mean(RowAggregateFunc):
    """
    Aggregation function to calculate the mean of values.
    """

    def __init__(self, func: Callable[[Schema], Union[int, float]], name: str = "mean"):
        """
        Initialize a Mean aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "mean")
        """

        def _finalize(x: dict[str, Any]) -> float:
            if x["count"] == 0:
                raise ValueError("Cannot compute result of empty dataset")
            return float(x["sum"]) / int(x["count"])

        super().__init__(
            initial_value={"sum": 0.0, "count": 0},
            accumulate=lambda x, y: {
                "sum": x["sum"] + func(y),
                "count": x["count"] + 1,
            },
            merge=lambda x, y: {
                "sum": x["sum"] + y["sum"],
                "count": x["count"] + y["count"],
            },
            finalize=_finalize,
            name=name,
        )


class Count(RowAggregateFunc):
    """
    Aggregation function to count the number of items.
    """

    def __init__(self, name: str = "count") -> None:
        """
        Initialize a Count aggregation function.

        Args:
            name: Name of the aggregation function (default: "count")
        """
        super().__init__(
            initial_value={"count": 0},
            accumulate=lambda x, y: {"count": x["count"] + 1},
            merge=lambda x, y: {"count": x["count"] + y["count"]},
            finalize=lambda x: x["count"],
            name=name,
        )


class Min(RowAggregateFunc):
    """
    Aggregation function to find the minimum value.
    """

    def __init__(
        self, func: Callable[[Schema], Union[int, float]], name: str = "min"
    ) -> None:
        """
        Initialize a Min aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "min")
        """

        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            if x["min"] is None:
                x["min"] = func(y)
            else:
                x["min"] = min(x["min"], func(y))
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            if x["min"] is None:
                x["min"] = y["min"]
            elif y["min"] is None:
                x["min"] = x["min"]
            else:
                x["min"] = min(x["min"], y["min"])
            return x

        def _finalize(x: dict[str, Any]) -> Union[int, float]:
            if x["min"] is None:
                raise ValueError("Cannot compute result of empty dataset")
            assert isinstance(x["min"], (int, float))
            return x["min"]

        super().__init__(
            initial_value={"min": None},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
            name=name,
        )


class Max(RowAggregateFunc):
    """
    Aggregation function to find the maximum value.
    """

    def __init__(
        self, func: Callable[[Schema], Union[int, float]], name: str = "max"
    ) -> None:
        """
        Initialize a Max aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "max")
        """

        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            if x["max"] is None:
                x["max"] = func(y)
            else:
                x["max"] = max(x["max"], func(y))
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            if x["max"] is None:
                x["max"] = y["max"]
            elif y["max"] is None:
                x["max"] = x["max"]
            else:
                x["max"] = max(x["max"], y["max"])
            return x

        def _finalize(x: dict[str, Any]) -> Union[int, float]:
            if x["max"] is None:
                raise ValueError("Cannot compute result of empty dataset")
            assert isinstance(x["max"], (int, float))
            return x["max"]

        super().__init__(
            initial_value={"max": None},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
            name=name,
        )


class Variance(RowAggregateFunc):
    """
    Aggregation function to calculate the sample variance of values.
    """

    def __init__(
        self, func: Callable[[Schema], Union[int, float]], name: str = "variance"
    ) -> None:
        """
        Initialize a Variance aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "variance")
        """

        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            v = func(y)
            x["count"] += 1
            x["sum"] += v
            x["sum_squared"] += v * v
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            x["count"] += y["count"]
            x["sum"] += y["sum"]
            x["sum_squared"] += y["sum_squared"]
            return x

        def _finalize(x: dict[str, Any]) -> float:
            n = int(x["count"])
            s = float(x["sum"])
            s2 = float(x["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample variance with fewer than two data points"
                )
            mean = s / n
            return (s2 - n * mean * mean) / (n - 1)

        super().__init__(
            initial_value={"sum": 0, "sum_squared": 0, "count": 0},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
            name=name,
        )


class StdDev(RowAggregateFunc):
    """
    Aggregation function to calculate the sample standard deviation of values.
    """

    def __init__(
        self, func: Callable[[Schema], Union[int, float]], name: str = "stddev"
    ) -> None:
        """
        Initialize a StdDev aggregation function.

        Args:
            func: Function that extracts a numeric value from each item
            name: Name of the aggregation function (default: "stddev")
        """

        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            v = func(y)
            x["count"] += 1
            x["sum"] += v
            x["sum_squared"] += v * v
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            x["count"] += y["count"]
            x["sum"] += y["sum"]
            x["sum_squared"] += y["sum_squared"]
            return x

        def _finalize(x: dict[str, Any]) -> float:
            n = int(x["count"])
            s = float(x["sum"])
            s2 = float(x["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample standard deviation with fewer than two data points"
                )
            mean = s / n
            sample_var = (s2 - n * mean * mean) / (n - 1)
            return math.sqrt(sample_var)

        super().__init__(
            initial_value={"sum": 0, "sum_squared": 0, "count": 0},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
            name=name,
        )


class Unique(RowAggregateFunc):
    """
    Aggregation function to find unique values.
    """

    def __init__(
        self, func: Callable[[Schema], set[Any]], name: str = "unique"
    ) -> None:
        """
        Initialize a Unique aggregation function.

        Args:
            func: Function that extracts a set of values from each item
            name: Name of the aggregation function (default: "unique")
        """

        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            x["unique"][func(y)] = True
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            x["unique"].update(y["unique"])
            return x

        def _finalize(x: dict[str, Any]) -> list[Any]:
            return list(x["unique"].keys())

        super().__init__(
            initial_value={"unique": {}},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
            name=name,
        )
