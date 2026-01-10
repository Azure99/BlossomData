from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from blossom.dataframe.aggregate import Count, Sum
from blossom.dataframe.spark_dataframe import SparkDataFrame
from blossom.schema.row_schema import RowSchema

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("blossom-spark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def _make_df(spark_session: SparkSession, values: list[int]) -> SparkDataFrame:
    rows = [RowSchema(data={"value": v}).to_dict() for v in values]
    rdd = spark_session.sparkContext.parallelize(rows, 1)
    return SparkDataFrame(rdd, spark_session)


def _values(items: list[RowSchema]) -> list[int]:
    return [int(item.data["value"]) for item in items]


def test_spark_dataframe_basic_ops(spark_session: SparkSession) -> None:
    df = _make_df(spark_session, [1, 2, 3])

    mapped = df.map(lambda s: RowSchema(data={"value": s.data["value"] + 1}))
    assert _values(mapped.collect()) == [2, 3, 4]

    filtered = df.filter(lambda s: s.data["value"] > 1)
    assert _values(filtered.collect()) == [2, 3]

    transformed = df.transform(lambda items: list(reversed(items)))
    assert _values(transformed.collect()) == [3, 2, 1]

    sorted_df = df.sort(lambda s: s.data["value"], ascending=False)
    assert _values(sorted_df.collect()) == [3, 2, 1]

    limited = df.limit(2)
    assert _values(limited.collect()) == [1, 2]

    shuffled = df.shuffle().collect()
    assert sorted(_values(shuffled)) == [1, 2, 3]

    repartitioned = df.repartition(2)
    assert repartitioned.spark_rdd.getNumPartitions() == 2

    splits = df.split(2)
    assert [len(s.collect()) for s in splits] == [2, 1]

    unioned = df.union(_make_df(spark_session, [4]))
    assert _values(unioned.collect()) == [1, 2, 3, 4]

    cached = df.cache()
    assert cached.spark_rdd.is_cached

    total = df.aggregate(Sum(lambda s: s.data["value"]))
    assert total == 6

    multi = df.aggregate(Sum(lambda s: s.data["value"]), Count())
    assert multi == {"sum": 6, "count": 3}

    grouped = df.group_by(lambda s: s.data["value"] % 2, name="parity").count()
    grouped_rows = grouped.collect()
    grouped_map = {row.data["parity"]: row.data["count"] for row in grouped_rows}
    assert grouped_map == {0: 1, 1: 2}

    base = SparkDataFrame(spark_session=spark_session)
    from_list_df = base.from_list([RowSchema(data={"value": 7})])
    assert _values(from_list_df.collect()) == [7]


def test_spark_dataframe_io(spark_session: SparkSession, write_jsonl, tmp_path) -> None:
    path = write_jsonl([{"a": 1}, {"a": 2}])
    df = SparkDataFrame(spark_session=spark_session).read_json(str(path))
    rows = df.collect()
    assert all(isinstance(row, RowSchema) for row in rows)
    assert sorted(row.data["a"] for row in rows) == [1, 2]

    out_path = tmp_path / "spark_out"
    df.write_json(str(out_path))

    lines: list[str] = []
    for part in out_path.glob("part-*"):
        lines.extend(part.read_text().splitlines())

    assert lines
    payloads = [json.loads(line) for line in lines]
    values = sorted(item["data"]["a"] for item in payloads)
    assert values == [1, 2]
    assert all(item["type"] == "row" for item in payloads)


def test_spark_dataframe_parquet_io(spark_session: SparkSession, tmp_path) -> None:
    df = _make_df(spark_session, [1, 2])
    out_path = tmp_path / "spark_parquet"
    df.write_parquet(str(out_path))

    loaded = SparkDataFrame(spark_session=spark_session).read_parquet(str(out_path))
    values = sorted(row.data["value"] for row in loaded.collect())
    assert values == [1, 2]
