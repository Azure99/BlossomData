from typing import Any

from pydantic import Field

from blossom.schema.schema import Schema

SCHEMA_TYPE_ROW = "row"


class RowSchema(Schema):
    """
    Schema for row data, representing a record with key-value pairs.

    This schema is used for processing structured data in a tabular format,
    where each row contains multiple fields with values.

    Attributes:
        type: Always "row"
        data: Dictionary containing the row data as key-value pairs
    """

    type: str = SCHEMA_TYPE_ROW
    data: dict[str, Any] = Field(default_factory=dict)

    def __getitem__(self, key: str) -> Any:
        """
        Access a field value using dictionary-like syntax.

        Args:
            key: The field name

        Returns:
            The value of the field

        Raises:
            KeyError: If the field does not exist
        """
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set a field value using dictionary-like syntax.

        Args:
            key: The field name
            value: The value to set
        """
        self.data[key] = value
