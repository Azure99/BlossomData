from typing import Any

from blossom.schema.base_schema import BaseSchema, SchemaType


class CustomSchema(BaseSchema):
    type: SchemaType = SchemaType.CUSTOM
    data: Any
