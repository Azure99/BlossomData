import random
from blossom.dataframe import AggregateFunc, Sum
from blossom.dataset import create_dataset
from blossom.schema import RowSchema

example_data = [
    RowSchema(
        data={
            "country": random.choice(["US", "CN", "JP", "KR", "TW", "HK"]),
            "score": random.randint(1, 100),
        }
    )
    for _ in range(1024)
]

count_cn_aggregate_func = AggregateFunc(
    initial_value=RowSchema(
        data={
            "cn_count": 0,
        }
    ),
    accumulate=lambda x, y: RowSchema(
        data={
            "cn_count": (
                x.data["cn_count"] + 1
                if y.data["country"] == "CN"
                else x.data["cn_count"]
            ),
        }
    ),
    merge=lambda x, y: RowSchema(
        data={
            "cn_count": x.data["cn_count"] + y.data["cn_count"],
        }
    ),
    finalize=lambda x: x.data["cn_count"],
)


dataset = create_dataset(example_data)
statistics = {
    "count_by_country": dataset.count_by_value(lambda x: x["country"]),
    "count": dataset.count(),
    "sum": dataset.sum(lambda x: x["score"]),
    "mean": dataset.mean(lambda x: x["score"]),
    "min": dataset.min(lambda x: x["score"]),
    "max": dataset.max(lambda x: x["score"]),
    "variance": dataset.variance(lambda x: x["score"]),
    "stddev": dataset.stddev(lambda x: x["score"]),
    "custom_aggregate": dataset.aggregate(Sum(lambda x: x["score"] * 2)),
    "custom_aggregate_func": dataset.aggregate(count_cn_aggregate_func),
}
print(statistics)
