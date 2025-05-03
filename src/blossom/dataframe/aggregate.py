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


class RowAggregateFunc(AggregateFunc[T]):
    def __init__(
        self,
        initial_value: dict[str, Any],
        accumulate: Callable[[dict[str, Any], Schema], dict[str, Any]],
        merge: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
        finalize: Optional[Callable[[dict[str, Any]], T]] = None,
    ):
        def _accumulate(x: Schema, y: Schema) -> Schema:
            assert isinstance(x, RowSchema)
            return RowSchema(data=accumulate(x.data, y))

        def _merge(x: Schema, y: Schema) -> Schema:
            assert isinstance(x, RowSchema)
            assert isinstance(y, RowSchema)
            return RowSchema(data=merge(x.data, y.data))

        def _finalize(x: Schema) -> T:
            assert isinstance(x, RowSchema)
            if finalize is None:
                return x.data  # type: ignore
            return finalize(x.data)

        super().__init__(RowSchema(data=initial_value), _accumulate, _merge, _finalize)


class Sum(RowAggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]):
        super().__init__(
            initial_value={"sum": 0},
            accumulate=lambda x, y: {"sum": x["sum"] + func(y)},
            merge=lambda x, y: {"sum": x["sum"] + y["sum"]},
            finalize=lambda x: x["sum"],
        )


class Mean(RowAggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]):
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
        )


class Count(RowAggregateFunc[int]):
    def __init__(self) -> None:
        super().__init__(
            initial_value={"count": 0},
            accumulate=lambda x, y: {"count": x["count"] + 1},
            merge=lambda x, y: {"count": x["count"] + y["count"]},
            finalize=lambda x: x["count"],
        )


class Min(RowAggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
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
        )


class Max(RowAggregateFunc[Union[int, float]]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
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
        )


class Variance(RowAggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
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
            S = float(x["sum"])
            S2 = float(x["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample variance with fewer than two data points"
                )
            mean = S / n
            return (S2 - n * mean * mean) / (n - 1)

        super().__init__(
            initial_value={"sum": 0, "sum_squared": 0, "count": 0},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class StdDev(RowAggregateFunc[float]):
    def __init__(self, func: Callable[[Schema], Union[int, float]]) -> None:
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
            S = float(x["sum"])
            S2 = float(x["sum_squared"])
            if n < SAMPLE_VARIANCE_MIN_DATA_POINTS:
                raise ValueError(
                    "Cannot compute sample standard deviation with fewer than two data points"
                )
            mean = S / n
            sample_var = (S2 - n * mean * mean) / (n - 1)
            return math.sqrt(sample_var)

        super().__init__(
            initial_value={"sum": 0, "sum_squared": 0, "count": 0},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )


class CountByValue(RowAggregateFunc[dict[Any, int]]):
    def __init__(self, func: Callable[[Schema], Any]) -> None:
        def _accumulate(x: dict[str, Any], y: Schema) -> dict[str, Any]:
            val = func(y)
            if val not in x["counter"]:
                x["counter"][val] = 0
            x["counter"][val] += 1
            return x

        def _merge(x: dict[str, Any], y: dict[str, Any]) -> dict[str, Any]:
            for val, count in y["counter"].items():
                if val not in x["counter"]:
                    x["counter"][val] = 0
                x["counter"][val] += count
            return x

        def _finalize(x: dict[str, Any]) -> dict[Any, int]:
            assert isinstance(x["counter"], dict)
            return x["counter"]

        super().__init__(
            initial_value={"counter": {}},
            accumulate=_accumulate,
            merge=_merge,
            finalize=_finalize,
        )
