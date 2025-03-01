from typing import Any

from blossom.schema.base_schema import BaseSchema, SchemaType


class CustomSchema(BaseSchema):
    type: str = SchemaType.CUSTOM
    data: Any
