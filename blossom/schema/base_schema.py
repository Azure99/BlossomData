import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SchemaType(Enum):
    BASE = "base"
    CHAT = "chat"
    TEXT = "text"
    CUSTOM = "custom"


class BaseSchema(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: SchemaType = SchemaType.BASE
    metadata: dict[str, Any] = {}
