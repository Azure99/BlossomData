from typing import Any

from blossom.schema.base_schema import BaseSchema


def load_schema_dict_list(data: list[dict[str, Any]]) -> list[BaseSchema]:
    return [BaseSchema.from_dict(item) for item in data]
