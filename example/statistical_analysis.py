import random
from blossom.dataframe import RowAggregateFunc, Sum, Mean, Count
from blossom.dataset import create_dataset
from blossom.schema import RowSchema
from blossom.op import EqualWidthBinner


example_data = [
    RowSchema(
        data={
            "country": random.choice(["US", "CN", "JP", "KR", "TW", "HK"]),
            "score": random.randint(1, 100),
        }
    )
    for _ in range(1024)
]


dataset = create_dataset(example_data)
statistics = {
    "count": dataset.count(),
    "min": dataset.min(lambda x: x["score"]),
    "max": dataset.max(lambda x: x["score"]),
    "unique": dataset.unique(lambda x: x["country"]),
    "agg": dataset.aggregate(
        Count(),
        Sum(lambda x: x["score"]),
        Sum(lambda x: x["score"] * 2, name="score_x2"),
        Mean(lambda x: x["score"]),
    ),
    "bin_score_count": [
        agg.data
        for agg in dataset.execute([EqualWidthBinner(lambda x: x["score"])])
        .group_by(lambda x: x.metadata["bin_label"], name="bin")
        .count()
        .sort(lambda x: x["bin"])
        .collect()
    ],
    "group_by_country_agg": [
        agg.data
        for agg in dataset.group_by(lambda x: x["country"])
        .aggregate(
            Count(),
            Sum(lambda x: x["score"]),
            Sum(lambda x: x["score"] * 2, name="score_x2"),
            Mean(lambda x: x["score"]),
        )
        .collect()
    ],
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
