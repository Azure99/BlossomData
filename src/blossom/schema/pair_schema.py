from typing import Any

from blossom.schema.schema import Schema

SCHEMA_TYPE_PAIR = "pair"


class PairSchema(Schema):
    type: str = SCHEMA_TYPE_PAIR
    key: Any
    value: Any
