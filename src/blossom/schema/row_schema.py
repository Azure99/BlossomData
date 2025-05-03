from typing import Any

from pydantic import Field

from blossom.schema.schema import Schema

SCHEMA_TYPE_ROW = "row"


class RowSchema(Schema):
    type: str = SCHEMA_TYPE_ROW
    data: dict[str, Any] = Field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value
