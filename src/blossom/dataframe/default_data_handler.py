from typing import Any

from blossom.dataframe.data_handler import DataHandler
from blossom.schema.row_schema import RowSchema
from blossom.schema.schema import FIELD_TYPE, Schema


class DefaultDataHandler(DataHandler):
    """
    Default implementation of DataHandler.

    This handler automatically detects schema types from dictionaries and
    creates appropriate schema objects. If no schema type is specified,
    it creates a RowSchema with the dictionary data.
    """

    def from_dict(self, data: dict[str, Any]) -> Schema:
        """
        Convert a dictionary to a schema object.

        If the dictionary has a "type" field, uses the appropriate schema class.
        Otherwise, creates a RowSchema with the dictionary data.

        Args:
            data: Dictionary to convert

        Returns:
            Schema object
        """
        schema_type = data.get(FIELD_TYPE)
        if schema_type:
            return Schema.from_dict(data)

        return RowSchema(data=data)

    def to_dict(self, schema: Schema) -> dict[str, Any]:
        """
        Convert a schema object to a dictionary.

        Uses the schema's to_dict method to perform the conversion.

        Args:
            schema: Schema object to convert

        Returns:
            Dictionary representation
        """
        return schema.to_dict()
