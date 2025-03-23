import uuid
from typing import Any, Optional, Type, TypeVar, ClassVar, cast

from pydantic import BaseModel, Field

T = TypeVar("T", bound="Schema")

FIELD_ID = "id"
FIELD_TYPE = "type"
FIELD_FAILED = "failed"
FIELD_METADATA = "metadata"
FIELD_DATA = "data"

SCHEMA_TYPE_BASE = "base"


class Schema(BaseModel):
    _schema_registry: ClassVar[dict[str, Type["Schema"]]] = {}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str = SCHEMA_TYPE_BASE
    failed: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @classmethod
    def from_dict(cls: Type[T], data: dict[str, Any]) -> T:
        if cls == Schema and FIELD_TYPE in data:
            schema_type = data.get(FIELD_TYPE)
            if schema_type not in cls._schema_registry:
                raise ValueError(f"Unsupported schema type: {schema_type}")

            schema_cls = cls._schema_registry[schema_type]
            return cast(T, schema_cls.model_validate(data))

        return cls.model_validate(data)

    @classmethod
    def register_schema(cls, schema_type: str, schema_cls: Type["Schema"]) -> None:
        cls._schema_registry[schema_type] = schema_cls

    @classmethod
    def get_schema_class(cls, schema_type: str) -> Optional[Type["Schema"]]:
        return cls._schema_registry.get(schema_type)

    @classmethod
    def get_registered_schemas(cls) -> dict[str, Type["Schema"]]:
        return cls._schema_registry.copy()
