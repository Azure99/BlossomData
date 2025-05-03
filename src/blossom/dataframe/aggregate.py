import copy
import math
from typing import Any, Callable, Optional, Union, TypeVar, Generic
from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import Schema

T = TypeVar("T")

SAMPLE_VARIANCE_MIN_DATA_POINTS = 2


class AggregateFunc(Generic[T]):
    def __init__(
        self,
        initial_value: Schema,
        accumulate: Callable[[Schema, Schema], Schema],
        merge: Callable[[Schema, Schema], Schema],
        finalize: Optional[Callable[[Schema], T]] = None,
    ):
        self._initial_value = initial_value
        self._accumulate = accumulate
        self._merge = merge
        self._finalize = finalize

    @property
    def initial_value(self) -> Schema:
        return copy.deepcopy(self._initial_value)

    def accumulate(self, accumulator: Schema, item: Schema) -> Schema:
        return self._accumulate(accumulator, item)

    def merge(self, accumulator1: Schema, accumulator2: Schema) -> Schema:
        return self._merge(accumulator1, accumulator2)

    def finalize(self, accumulator: Schema) -> T:
        if self._finalize is None:
            return accumulator  # type: ignore
        return self._finalize(accumulator)


class Sum(AggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]):
        def _accumulate(x: Schema, y: Schema) -> Schema:
            x.metadata["sum"] += func(y)
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            x.metadata["sum"] += y.metadata["sum"]
            return x

        super().__init__(
            initial_value=RowSchema(metadata={"sum": 0}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=lambda x: x.metadata["sum"],
        )


class Mean(AggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]):
        def _accumulate(x: Schema, y: Schema) -> Schema:
            x.metadata["sum"] += func(y)
            x.metadata["count"] += 1
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            x.metadata["sum"] += y.metadata["sum"]
            x.metadata["count"] += y.metadata["count"]
            return x

        def _finalize(x: Schema) -> float:
            if x.metadata["count"] == 0:
                raise ValueError("Cannot compute result of empty dataset")
            return float(x.metadata["sum"]) / int(x.metadata["count"])

        super().__init__(
            initial_value=RowSchema(metadata={"sum": 0.0, "count": 0}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class Count(AggregateFunc[int]):
    def __init__(self) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            x.metadata["count"] += 1
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            x.metadata["count"] += y.metadata["count"]
            return x

        super().__init__(
            initial_value=RowSchema(metadata={"count": 0}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=lambda x: x.metadata["count"],
        )


class Min(AggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            if x.metadata["min"] is None:
                x.metadata["min"] = func(y)
            else:
                x.metadata["min"] = min(x.metadata["min"], func(y))
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            if x.metadata["min"] is None:
                x.metadata["min"] = y.metadata["min"]
            elif y.metadata["min"] is None:
                x.metadata["min"] = x.metadata["min"]
            else:
                x.metadata["min"] = min(x.metadata["min"], y.metadata["min"])
            return x

        def _finalize(x: Schema) -> Union[int, float]:
            if x.metadata["min"] is None:
                raise ValueError("Cannot compute result of empty dataset")
            assert isinstance(x.metadata["min"], (int, float))
            return x.metadata["min"]

        super().__init__(
            initial_value=RowSchema(metadata={"min": None}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class Max(AggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            if x.metadata["max"] is None:
                x.metadata["max"] = func(y)
            else:
                x.metadata["max"] = max(x.metadata["max"], func(y))
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            if x.metadata["max"] is None:
                x.metadata["max"] = y.metadata["max"]
            elif y.metadata["max"] is None:
                x.metadata["max"] = x.metadata["max"]
            else:
                x.metadata["max"] = max(x.metadata["max"], y.metadata["max"])
            return x

        def _finalize(x: Schema) -> Union[int, float]:
            if x.metadata["max"] is None:
                raise ValueError("Cannot compute result of empty dataset")
            assert isinstance(x.metadata["max"], (int, float))
            return x.metadata["max"]

        super().__init__(
            initial_value=RowSchema(metadata={"max": None}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class Variance(AggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            v = func(y)
            x.metadata["count"] += 1
            x.metadata["sum"] += v
            x.metadata["sum_squared"] += v * v
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            x.metadata["count"] += y.metadata["count"]
            x.metadata["sum"] += y.metadata["sum"]
            x.metadata["sum_squared"] += y.metadata["sum_squared"]
            return x

        def _finalize(x: Schema) -> float:
            n = int(x.metadata["count"])
            S = float(x.metadata["sum"])
            S2 = float(x.metadata["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample variance with fewer than two data points"
                )
            mean = S / n
            return (S2 - n * mean * mean) / (n - 1)

        super().__init__(
            initial_value=RowSchema(metadata={"sum": 0, "sum_squared": 0, "count": 0}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class StdDev(AggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            v = func(y)
            x.metadata["count"] += 1
            x.metadata["sum"] += v
            x.metadata["sum_squared"] += v * v
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            x.metadata["count"] += y.metadata["count"]
            x.metadata["sum"] += y.metadata["sum"]
            x.metadata["sum_squared"] += y.metadata["sum_squared"]
            return x

        def _finalize(x: Schema) -> float:
            n = int(x.metadata["count"])
            S = float(x.metadata["sum"])
            S2 = float(x.metadata["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample standard deviation with fewer than two data points"
                )
            mean = S / n
            sample_var = (S2 - n * mean * mean) / (n - 1)
            return math.sqrt(sample_var)

        super().__init__(
            initial_value=RowSchema(metadata={"sum": 0, "sum_squared": 0, "count": 0}),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class CountByValue(AggregateFunc[dict[Any, int]]):
    def __init__(self, func: Callable[[Schema], Any]) -> None:
        def _accumulate(x: Schema, y: Schema) -> Schema:
            val = func(y)
            if val not in x.metadata:
                x.metadata[val] = 0
            x.metadata[val] += 1
            return x

        def _merge(x: Schema, y: Schema) -> Schema:
            for val, count in y.metadata.items():
                if val not in x.metadata:
                    x.metadata[val] = 0
                x.metadata[val] += count
            return x

        def _finalize(x: Schema) -> dict[Any, int]:
            return x.metadata

        super().__init__(
            initial_value=RowSchema(),
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )
