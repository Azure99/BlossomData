from abc import ABC, abstractmethod
from typing import Any, cast

from blossom.context.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.schema.chat_schema import ChatSchema
from blossom.schema.custom_schema import CustomSchema
from blossom.schema.schema import Schema
from blossom.schema.text_schema import TextSchema


class Operator(ABC):
    """
    Abstract base class for all operators.

    Operators are the building blocks for data processing pipelines,
    implementing specific transformations or filters on data.
    """

    def __init__(self) -> None:
        """
        Initialize the operator.

        The context will be set later by the execution framework.
        """
        self.context: Context

    @abstractmethod
    def process(self, dataframe: DataFrame) -> DataFrame:
        """
        Process the dataframe with this operator.

        This is the main method that must be implemented by all operators.

        Args:
            dataframe: Input dataframe to process

        Returns:
            Processed dataframe
        """
        pass

    def init_context(self, context: Context) -> None:
        """
        Initialize the operator with an execution context.

        Args:
            context: The execution context
        """
        self.context = context

    @staticmethod
    def _cast_base(data: Any) -> Schema:
        """
        Cast data to the base Schema type with runtime type checking.

        Args:
            data: Data to cast

        Returns:
            Data cast to Schema

        Raises:
            AssertionError: If data is not a Schema
        """
        assert isinstance(data, Schema)
        return data

    @staticmethod
    def _cast_base_list(data: list[Any]) -> list[Schema]:
        """
        Cast a list of data to a list of Schema with runtime type checking.

        Args:
            data: List of data to cast

        Returns:
            List of data cast to Schema

        Raises:
            AssertionError: If any item is not a Schema
        """
        for d in data:
            assert isinstance(d, Schema)
        return cast(list[Schema], data)

    @staticmethod
    def _cast_chat(data: Any) -> ChatSchema:
        """
        Cast data to ChatSchema with runtime type checking.

        Args:
            data: Data to cast

        Returns:
            Data cast to ChatSchema

        Raises:
            AssertionError: If data is not a ChatSchema
        """
        assert isinstance(data, ChatSchema)
        return data

    @staticmethod
    def _cast_chat_list(data: list[Schema]) -> list[ChatSchema]:
        """
        Cast a list of data to a list of ChatSchema with runtime type checking.

        Args:
            data: List of data to cast

        Returns:
            List of data cast to ChatSchema

        Raises:
            AssertionError: If any item is not a ChatSchema
        """
        for d in data:
            assert isinstance(d, ChatSchema)
        return cast(list[ChatSchema], data)

    @staticmethod
    def _cast_custom(data: Any) -> CustomSchema:
        """
        Cast data to CustomSchema with runtime type checking.

        Args:
            data: Data to cast

        Returns:
            Data cast to CustomSchema

        Raises:
            AssertionError: If data is not a CustomSchema
        """
        assert isinstance(data, CustomSchema)
        return data

    @staticmethod
    def _cast_custom_list(data: list[Schema]) -> list[CustomSchema]:
        """
        Cast a list of data to a list of CustomSchema with runtime type checking.

        Args:
            data: List of data to cast

        Returns:
            List of data cast to CustomSchema

        Raises:
            AssertionError: If any item is not a CustomSchema
        """
        for d in data:
            assert isinstance(d, CustomSchema)
        return cast(list[CustomSchema], data)

    @staticmethod
    def _cast_text(data: Any) -> TextSchema:
        """
        Cast data to TextSchema with runtime type checking.

        Args:
            data: Data to cast

        Returns:
            Data cast to TextSchema

        Raises:
            AssertionError: If data is not a TextSchema
        """
        assert isinstance(data, TextSchema)
        return data

    @staticmethod
    def _cast_text_list(data: list[Schema]) -> list[TextSchema]:
        """
        Cast a list of data to a list of TextSchema with runtime type checking.

        Args:
            data: List of data to cast

        Returns:
            List of data cast to TextSchema

        Raises:
            AssertionError: If any item is not a TextSchema
        """
        for d in data:
            assert isinstance(d, TextSchema)
        return cast(list[TextSchema], data)
