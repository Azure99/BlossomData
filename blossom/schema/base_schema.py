import uuid
from enum import Enum
from typing import Any, Optional, Type, TypeVar
import json

from pydantic import BaseModel, Field


class SchemaType(Enum):
    BASE = "base"
    CHAT = "chat"
    TEXT = "text"
    CUSTOM = "custom"


T = TypeVar("T", bound="BaseSchema")


class BaseSchema(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    failed: Optional[bool] = None
    type: SchemaType = Field(default=SchemaType.BASE)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls: Type[T], data: dict[str, Any]) -> T:
        return cls.model_validate(data)

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        data = json.loads(json_str)
        return cls.from_dict(data)
