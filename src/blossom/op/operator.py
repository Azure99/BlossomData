from abc import ABC, abstractmethod
from typing import Any, cast

from blossom.context.context import Context
from blossom.dataframe.dataframe import DataFrame
from blossom.schema.chat_schema import ChatSchema
from blossom.schema.custom_schema import CustomSchema
from blossom.schema.schema import Schema
from blossom.schema.text_schema import TextSchema


class Operator(ABC):
    def __init__(self) -> None:
        self.context: Context

    @abstractmethod
    def process(self, dataframe: DataFrame) -> DataFrame:
        pass

    def init_context(self, context: Context) -> None:
        self.context = context

    @staticmethod
    def _cast_base(data: Any) -> Schema:
        assert isinstance(data, Schema)
        return data

    @staticmethod
    def _cast_base_list(data: list[Any]) -> list[Schema]:
        for d in data:
            assert isinstance(d, Schema)
        return cast(list[Schema], data)

    @staticmethod
    def _cast_chat(data: Any) -> ChatSchema:
        assert isinstance(data, ChatSchema)
        return data

    @staticmethod
    def _cast_chat_list(data: list[Schema]) -> list[ChatSchema]:
        for d in data:
            assert isinstance(d, ChatSchema)
        return cast(list[ChatSchema], data)

    @staticmethod
    def _cast_custom(data: Any) -> CustomSchema:
        assert isinstance(data, CustomSchema)
        return data

    @staticmethod
    def _cast_custom_list(data: list[Schema]) -> list[CustomSchema]:
        for d in data:
            assert isinstance(d, CustomSchema)
        return cast(list[CustomSchema], data)

    @staticmethod
    def _cast_text(data: Any) -> TextSchema:
        assert isinstance(data, TextSchema)
        return data

    @staticmethod
    def _cast_text_list(data: list[Schema]) -> list[TextSchema]:
        for d in data:
            assert isinstance(d, TextSchema)
        return cast(list[TextSchema], data)
