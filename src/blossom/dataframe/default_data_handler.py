from typing import Any

from blossom.dataframe.data_handler import DataHandler
from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import FIELD_TYPE, Schema


class DefaultDataHandler(DataHandler):
    def from_dict(self, data: dict[str, Any]) -> Schema:
        schema_type = data.get(FIELD_TYPE)
        if schema_type:
            return Schema.from_dict(data)

        return RowSchema(data=data)

    def to_dict(self, schema: Schema) -> dict[str, Any]:
        return schema.to_dict()
