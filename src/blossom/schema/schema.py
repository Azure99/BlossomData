import uuid
from typing import Any, Optional, Type, TypeVar, ClassVar, cast

from pydantic import BaseModel, Field, field_validator

T = TypeVar("T", bound="Schema")

FIELD_ID = "id"
FIELD_TYPE = "type"
FIELD_FAILED = "failed"
FIELD_METADATA = "metadata"
FIELD_DATA = "data"

SCHEMA_TYPE_BASE = "base"


class Schema(BaseModel):
    """
    Base schema class for all data types.

    This class serves as the foundation for all data schemas in the framework,
    providing common fields and functionality for data identification, type handling,
    failure tracking, and metadata storage.
    """

    _schema_registry: ClassVar[dict[str, Type["Schema"]]] = {}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    """Unique identifier for each schema instance"""

    type: str = SCHEMA_TYPE_BASE
    """Type identifier for the schema, used for serialization/deserialization"""

    failed: bool = Field(default=False)
    """Flag indicating whether processing of this item has failed"""

    metadata: dict[str, Any] = Field(default_factory=dict)
    """Additional metadata that can be attached to the schema"""

    @field_validator("failed", mode="before")
    @classmethod
    def convert_null_failed(cls, v: Any) -> bool:
        """Convert None values to False for the failed field"""
        return False if v is None else v

    @field_validator("metadata", mode="before")
    @classmethod
    def convert_null_metadata(cls, v: Any) -> dict[str, Any]:
        """Convert None values to empty dict for the metadata field"""
        return {} if v is None else v

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the schema to a dictionary representation.

        Returns:
            dict[str, Any]: Dictionary representation of the schema
        """
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls: Type[T], data: dict[str, Any]) -> T:
        """
        Create a schema instance from a dictionary.

        If the class is Schema and the dictionary contains a type field,
        the appropriate schema subclass will be used based on the type.

        Args:
            data: Dictionary containing schema data

        Returns:
            An instance of the appropriate schema class

        Raises:
            ValueError: If the schema type is not registered
        """
        if cls == Schema and FIELD_TYPE in data:
            schema_type = data.get(FIELD_TYPE)
            if schema_type not in cls._schema_registry:
                raise ValueError(f"Unsupported schema type: {schema_type}")

            schema_cls = cls._schema_registry[schema_type]
            return cast(T, schema_cls.model_validate(data))

        return cls.model_validate(data)

    @classmethod
    def register_schema(cls, schema_type: str, schema_cls: Type["Schema"]) -> None:
        """
        Register a schema class with its type identifier.

        Args:
            schema_type: Type identifier for the schema
            schema_cls: Schema class to register
        """
        cls._schema_registry[schema_type] = schema_cls

    @classmethod
    def get_schema_class(cls, schema_type: str) -> Optional[Type["Schema"]]:
        """
        Get a schema class by its type identifier.

        Args:
            schema_type: Type identifier for the schema

        Returns:
            The schema class if registered, None otherwise
        """
        return cls._schema_registry.get(schema_type)

    @classmethod
    def get_registered_schemas(cls) -> dict[str, Type["Schema"]]:
        """
        Get all registered schema classes.

        Returns:
            Dictionary mapping schema type identifiers to schema classes
        """
        return cls._schema_registry.copy()
