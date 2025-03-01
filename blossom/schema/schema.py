import uuid
from typing import Any, Dict, Optional, Type, TypeVar, ClassVar, cast
import json
from pydantic import BaseModel, Field
from blossom.util.type import StrEnum

T = TypeVar("T", bound="Schema")

FIELD_ID = "id"
FIELD_FAILED = "failed"
FIELD_TYPE = "type"
FIELD_METADATA = "metadata"
FIELD_DATA = "data"


class SchemaType(StrEnum):
    BASE = "base"
    CUSTOM = "custom"
    CHAT = "chat"
    TEXT = "text"


class Schema(BaseModel):
    _schema_registry: ClassVar[Dict[str, Type["Schema"]]] = {}

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    failed: Optional[bool] = None
    type: str = SchemaType.BASE
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_row(self) -> dict[str, Any]:
        data = self.model_dump()
        row = {
            FIELD_ID: data.pop(FIELD_ID),
            FIELD_FAILED: data.pop(FIELD_FAILED),
            FIELD_TYPE: data.pop(FIELD_TYPE),
            FIELD_METADATA: data.pop(FIELD_METADATA),
        }
        row[FIELD_DATA] = json.dumps(data, ensure_ascii=False)
        return row

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
    def from_json(cls: Type[T], json_str: str) -> T:
        data = json.loads(json_str)
        return cls.from_dict(data)

    @classmethod
    def from_row(cls: Type[T], row: dict[str, Any]) -> T:
        data = json.loads(row[FIELD_DATA])
        data[FIELD_ID] = row[FIELD_ID]
        data[FIELD_FAILED] = row[FIELD_FAILED]
        data[FIELD_TYPE] = row[FIELD_TYPE]
        data[FIELD_METADATA] = row[FIELD_METADATA]
        return cls.from_dict(data)

    @classmethod
    def register_schema(cls, schema_type: str, schema_cls: Type["Schema"]) -> None:
        cls._schema_registry[schema_type] = schema_cls

    @classmethod
    def get_schema_class(cls, schema_type: str) -> Optional[Type["Schema"]]:
        return cls._schema_registry.get(schema_type)

    @classmethod
    def get_registered_schemas(cls) -> Dict[str, Type["Schema"]]:
        return cls._schema_registry.copy()
