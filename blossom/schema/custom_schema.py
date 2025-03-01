from typing import Any

from blossom.schema.schema import Schema, SchemaType


class CustomSchema(Schema):
    type: str = SchemaType.CUSTOM
    data: Any
