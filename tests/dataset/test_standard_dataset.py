from __future__ import annotations

from blossom.dataframe.aggregate import Sum
from blossom.dataset import DatasetEngine, create_dataset
from blossom.op.map_operator import map_operator
from blossom.schema.text_schema import TextSchema


@map_operator()
def _uppercase(item: TextSchema) -> TextSchema:
    item.content = item.content.upper()
    return item


def test_standard_dataset_execute_and_collect() -> None:
    data = [TextSchema(content="a"), TextSchema(content="bb")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    result = dataset.execute([_uppercase]).collect()
    assert [item.content for item in result] == ["A", "BB"]


def test_standard_dataset_aggregate_group_by() -> None:
    data = [TextSchema(content="a"), TextSchema(content="bb"), TextSchema(content="c")]
    dataset = create_dataset(data, engine=DatasetEngine.LOCAL)
    dataset = dataset.add_metadata(lambda x: {"len": len(x.content)})

    total = dataset.aggregate(Sum(lambda x: x.metadata["len"]))
    expected_total = 4
    assert total == expected_total

    grouped = dataset.group_by(lambda x: x.metadata["len"], name="len").count()
    rows = grouped.collect()
    grouped_counts = {row.data["len"]: row.data["count"] for row in rows}
    assert grouped_counts == {1: 2, 2: 1}
