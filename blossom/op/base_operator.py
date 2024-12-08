from typing import Any, cast

from blossom.context.context import Context
from blossom.schema.base_schema import BaseSchema
from blossom.schema.chat_schema import ChatSchema
from blossom.schema.custom_schema import CustomSchema
from blossom.schema.text_schema import TextSchema


class BaseOperator:
    def __init__(self) -> None:
        self.context: Context

    def process(self, data: list[BaseSchema]) -> list[BaseSchema]:
        raise NotImplementedError("Operator not implemented")

    def init_context(self, context: Context) -> None:
        self.context = context

    @staticmethod
    def _cast_base(data: Any) -> BaseSchema:
        assert isinstance(data, BaseSchema)
        return data

    @staticmethod
    def _cast_base_list(data: list[Any]) -> list[BaseSchema]:
        for d in data:
            assert isinstance(d, BaseSchema)
        return cast(list[BaseSchema], data)

    @staticmethod
    def _cast_chat(data: Any) -> ChatSchema:
        assert isinstance(data, ChatSchema)
        return data

    @staticmethod
    def _cast_chat_list(data: list[BaseSchema]) -> list[ChatSchema]:
        for d in data:
            assert isinstance(d, ChatSchema)
        return cast(list[ChatSchema], data)

    @staticmethod
    def _cast_custom(data: Any) -> CustomSchema:
        assert isinstance(data, CustomSchema)
        return data

    @staticmethod
    def _cast_custom_list(data: list[BaseSchema]) -> list[CustomSchema]:
        for d in data:
            assert isinstance(d, CustomSchema)
        return cast(list[CustomSchema], data)

    @staticmethod
    def _cast_text(data: Any) -> TextSchema:
        assert isinstance(data, TextSchema)
        return data

    @staticmethod
    def _cast_text_list(data: list[BaseSchema]) -> list[TextSchema]:
        for d in data:
            assert isinstance(d, TextSchema)
        return cast(list[TextSchema], data)
