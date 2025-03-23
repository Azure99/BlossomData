from typing import Any

from blossom.schema.schema import Schema

SCHEMA_TYPE_CUSTOM = "custom"


class CustomSchema(Schema):
    type: str = SCHEMA_TYPE_CUSTOM
    data: Any
