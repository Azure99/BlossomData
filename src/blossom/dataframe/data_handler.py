from abc import ABC, abstractmethod
from typing import Any

from blossom.schema.schema import Schema, FIELD_TYPE
from blossom.schema.row_schema import RowSchema


class DataHandler(ABC):
    """
    Abstract base class for data handlers.

    A DataHandler is responsible for converting between schema objects and
    dictionary representations, enabling custom serialization and deserialization
    for different data formats and structures.
    """

    def from_dict_list(self, data: list[dict[str, Any]]) -> list[Schema]:
        """
        Convert a list of dictionaries to a list of schema objects.

        Args:
            data: List of dictionaries to convert

        Returns:
            List of schema objects
        """
        return [self.from_dict(row) for row in data]

    def to_dict_list(self, data: list[Schema]) -> list[dict[str, Any]]:
        """
        Convert a list of schema objects to a list of dictionaries.

        Args:
            data: List of schema objects to convert

        Returns:
            List of dictionaries
        """
        return [self.to_dict(row) for row in data]

    @abstractmethod
    def from_dict(self, data: dict[str, Any]) -> Schema:
        """
        Convert a dictionary to a schema object.

        Args:
            data: Dictionary to convert

        Returns:
            Schema object
        """
        pass

    @abstractmethod
    def to_dict(self, schema: Schema) -> dict[str, Any]:
        """
        Convert a schema object to a dictionary.

        Args:
            schema: Schema object to convert

        Returns:
            Dictionary representation
        """
        pass


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


class DictDataHandler(DataHandler):
    """
    Simple data handler that works directly with dictionary data.

    This handler converts between dictionaries and RowSchema objects without
    any additional processing. It's useful for working with plain dictionary data
    that doesn't require complex schema transformations.
    """

    def __init__(self, preserve_metadata: bool = False):
        """
        Initialize a DictDataHandler.

        Args:
            preserve_metadata: If True, include schema metadata in the output dictionary
                               when converting from Schema to dict. Default is False.
        """
        self.preserve_metadata = preserve_metadata

    def from_dict(self, data: dict[str, Any]) -> Schema:
        """
        Convert a dictionary to a RowSchema object.

        Simply wraps the dictionary in a RowSchema without any transformation.

        Args:
            data: Dictionary to convert

        Returns:
            RowSchema containing the input dictionary
        """
        return RowSchema(data=data)

    def to_dict(self, schema: Schema) -> dict[str, Any]:
        """
        Convert a Schema object to a dictionary.

        Args:
            schema: Schema object to convert, must be a RowSchema instance

        Returns:
            Dictionary representation. If preserve_metadata is True, the result includes
            both schema metadata and data. Otherwise, only returns the schema data.
        """
        assert isinstance(schema, RowSchema)

        if not self.preserve_metadata:
            return schema.data

        result = {}
        result.update(schema.metadata)
        result.update(schema.data)
        return result
