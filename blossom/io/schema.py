from typing import Any
from blossom.schema.base_schema import BaseSchema, SchemaType
from blossom.schema.chat_schema import ChatSchema
from blossom.schema.custom_schema import CustomSchema
from blossom.schema.text_schema import TextSchema


def load_schema_dict(data: dict[str, Any]) -> BaseSchema:
    type = data.get("type", None)
    if not type:
        raise ValueError("Schema type is required")

    if type == SchemaType.CHAT.value:
        return ChatSchema(**data)
    elif type == SchemaType.TEXT.value:
        return TextSchema(**data)
    elif type == SchemaType.CUSTOM.value:
        return CustomSchema(**data)
    else:
        raise ValueError(f"Unsupported schema type: {type}")


def load_schema_dict_list(data: list[dict[str, Any]]) -> list[BaseSchema]:
    return [load_schema_dict(item) for item in data]
