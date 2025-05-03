import random
from blossom.dataframe import RowAggregateFunc, Sum
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


dataset = create_dataset(example_data, engine="spark")
statistics = {
    "count": dataset.count(),
    "sum": dataset.sum(lambda x: x["score"]),
    "mean": dataset.mean(lambda x: x["score"]),
    "min": dataset.min(lambda x: x["score"]),
    "max": dataset.max(lambda x: x["score"]),
    "variance": dataset.variance(lambda x: x["score"]),
    "stddev": dataset.stddev(lambda x: x["score"]),
    "unique": dataset.unique(lambda x: x["country"]),
    "group_by_country_count": [
        {
            "country": kv.key,
            "count": kv.value,
        }
        for kv in dataset.group_by(lambda x: x["country"]).count().collect()
    ],
    "custom_aggregate": dataset.aggregate(Sum(lambda x: x["score"] * 2)),
    "custom_aggregate_func": dataset.aggregate(
        RowAggregateFunc(
            initial_value={"cn_count": 0},
            accumulate=lambda x, y: {
                "cn_count": (
                    x["cn_count"] + 1 if y["country"] == "CN" else x["cn_count"]
                ),
            },
            merge=lambda x, y: {
                "cn_count": x["cn_count"] + y["cn_count"],
            },
            finalize=lambda x: x["cn_count"],
        ),
    ),
}
print(statistics)
