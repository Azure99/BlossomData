from abc import ABC, abstractmethod
from typing import Any

from blossom.schema.schema import Schema


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
