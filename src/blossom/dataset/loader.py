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
    """
    Enumeration of supported data file types.

    Attributes:
        JSON: JSON file format
    """

    JSON = "json"


class DatasetEngine(StrEnum):
    """
    Enumeration of available dataset processing engines.

    Attributes:
        LOCAL: Local in-memory processing engine
        SPARK: Apache Spark distributed processing engine
        RAY: Ray distributed processing engine
    """

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
    """
    Load a dataset from a file.

    Args:
        path: Path to the data file
        engine: Processing engine to use (default: local)
        data_type: Type of data file (default: json)
        data_handler: Optional custom data handler for deserialization
        context: Optional execution context
        spark_session: Optional Spark session (required for Spark engine)

    Returns:
        A dataset containing the loaded data

    Raises:
        ValueError: If an invalid engine or data type is specified
    """
    dataframe: DataFrame
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
    data: Optional[list[Schema]] = None,
    engine: str = DatasetEngine.LOCAL,
    context: Optional[Context] = None,
    spark_session: Optional[SparkSession] = None,
) -> Dataset:
    """
    Create a dataset from a list of schemas.

    Args:
        data: List of schema objects (default: empty list)
        engine: Processing engine to use (default: local)
        context: Optional execution context
        spark_session: Optional Spark session (required for Spark engine)

    Returns:
        A dataset containing the provided data

    Raises:
        ValueError: If an invalid engine is specified
    """
    dataframe: DataFrame
    if engine == DatasetEngine.LOCAL:
        dataframe = LocalDataFrame()
    elif engine == DatasetEngine.RAY:
        dataframe = RayDataFrame()
    elif engine == DatasetEngine.SPARK:
        dataframe = SparkDataFrame(spark_session=spark_session)
    else:
        raise ValueError(f"Invalid dataset engine: {engine}")

    dataframe = dataframe.from_list(data or [])
    return StandardDataset(context, dataframe)
