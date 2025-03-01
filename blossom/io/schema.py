from typing import Any

from blossom.schema.schema import Schema


def load_schema_dict_list(data: list[dict[str, Any]]) -> list[Schema]:
    return [Schema.from_dict(item) for item in data]
