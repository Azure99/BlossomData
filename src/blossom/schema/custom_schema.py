from typing import Any

from blossom.schema.schema import Schema

SCHEMA_TYPE_CUSTOM = "custom"


class CustomSchema(Schema):
    """
    Schema for custom data, allowing for arbitrary data types.

    This schema provides flexibility for data that doesn't fit into the other schema types,
    allowing users to store and process any type of data.

    Attributes:
        type: Always "custom"
        data: Any data value or object
    """

    type: str = SCHEMA_TYPE_CUSTOM
    data: Any
