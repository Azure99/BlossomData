import uuid
from typing import Any, Dict, Optional, Type, TypeVar, ClassVar, cast
import json
from pydantic import BaseModel, Field
from blossom.util.type import StrEnum

TYPE_FIELD = "type"
T = TypeVar("T", bound="BaseSchema")


class SchemaType(StrEnum):
    BASE = "base"
    CUSTOM = "custom"
    CHAT = "chat"
    TEXT = "text"


class BaseSchema(BaseModel):
    _schema_registry: ClassVar[Dict[str, Type["BaseSchema"]]] = {}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    failed: Optional[bool] = None
    type: str = SchemaType.BASE
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls: Type[T], data: dict[str, Any]) -> T:
        if cls == BaseSchema and TYPE_FIELD in data:
            schema_type = data.get(TYPE_FIELD)
            if schema_type not in cls._schema_registry:
                raise ValueError(f"Unsupported schema type: {schema_type}")

            schema_cls = cls._schema_registry[schema_type]
            return cast(T, schema_cls.model_validate(data))

        return cls.model_validate(data)

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def register_schema(cls, schema_type: str, schema_cls: Type["BaseSchema"]) -> None:
        cls._schema_registry[schema_type] = schema_cls

    @classmethod
    def get_schema_class(cls, schema_type: str) -> Optional[Type["BaseSchema"]]:
        return cls._schema_registry.get(schema_type)

    @classmethod
    def get_registered_schemas(cls) -> Dict[str, Type["BaseSchema"]]:
        return cls._schema_registry.copy()
