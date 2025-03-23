from typing import Optional

from pyspark.sql import SparkSession

from blossom.context.context import Context
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.dataframe.ray_dataframe import RayDataFrame
from blossom.dataframe.spark_dataframe import SparkDataFrame
from blossom.dataset.dataset import Dataset
from blossom.dataset.standard_dataset import StandardDataset
from blossom.schema.schema import Schema
from blossom.util.type import StrEnum


class DataType(StrEnum):
    JSON = "json"


class DatasetEngine(StrEnum):
    LOCAL = "local"
    SPARK = "spark"
    RAY = "ray"


def load_dataset(
    path: str,
    engine: str = DatasetEngine.LOCAL,
    data_type: str = DataType.JSON,
    data_handler: Optional[DataHandler] = None,
    context: Optional[Context] = None,
    spark_session: Optional[SparkSession] = None,
) -> Dataset:
    dataframe: DataFrame = LocalDataFrame()
    if engine == DatasetEngine.LOCAL:
        dataframe = LocalDataFrame()
    elif engine == DatasetEngine.RAY:
        dataframe = RayDataFrame()
    elif engine == DatasetEngine.SPARK:
        dataframe = SparkDataFrame(spark_session=spark_session)
    else:
        raise ValueError(f"Invalid dataset engine: {engine}")

    if data_type == DataType.JSON:
        dataframe = dataframe.read_json(path, data_handler)
    else:
        raise ValueError(f"Invalid file type: {data_type}")

    return StandardDataset(context, dataframe)


def create_dataset(
    data: list[Schema],
    engine: str = DatasetEngine.LOCAL,
    context: Optional[Context] = None,
    spark_session: Optional[SparkSession] = None,
) -> Dataset:
    dataframe: DataFrame = LocalDataFrame()
    if engine == DatasetEngine.LOCAL:
        dataframe = LocalDataFrame()
    elif engine == DatasetEngine.RAY:
        dataframe = RayDataFrame()
    elif engine == DatasetEngine.SPARK:
        dataframe = SparkDataFrame(spark_session=spark_session)
    else:
        raise ValueError(f"Invalid dataset engine: {engine}")

    dataframe = dataframe.from_list(data)
    return StandardDataset(context, dataframe)
