from typing import Optional

from pyspark.sql import SparkSession

from blossom.context.context import Context
from blossom.dataframe.data_handler import DataHandler
from blossom.dataframe.dataframe import DataFrame
from blossom.dataframe.local_dataframe import LocalDataFrame
from blossom.dataframe.spark_dataframe import SparkDataFrame
from blossom.dataset.dataset import Dataset, FileType, DatasetType
from blossom.schema.schema import Schema


def load_dataset(
    path: str,
    type: str = DatasetType.LOCAL,
    file_type: str = FileType.JSON,
    data_handler: Optional[DataHandler] = None,
    context: Optional[Context] = None,
    spark_session: Optional[SparkSession] = None,
) -> Dataset:
    dataframe: DataFrame = LocalDataFrame()
    if type == DatasetType.LOCAL:
        dataframe = LocalDataFrame()
    elif type == DatasetType.SPARK:
        dataframe = SparkDataFrame(spark_session=spark_session)
    else:
        raise ValueError(f"Invalid dataset type: {type}")

    if file_type == FileType.JSON:
        dataframe = dataframe.read_json(path, data_handler)
    else:
        raise ValueError(f"Invalid file type: {file_type}")

    return Dataset(dataframe, context)


def create_dataset(
    data: list[Schema],
    type: str = DatasetType.LOCAL,
    context: Optional[Context] = None,
    spark_session: Optional[SparkSession] = None,
) -> Dataset:
    dataframe: DataFrame = LocalDataFrame()
    if type == DatasetType.LOCAL:
        dataframe = LocalDataFrame()
    elif type == DatasetType.SPARK:
        dataframe = SparkDataFrame(spark_session=spark_session)
    else:
        raise ValueError(f"Invalid dataset type: {type}")

    dataframe = dataframe.from_list(data)
    return Dataset(dataframe, context)
