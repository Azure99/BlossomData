import uuid
from enum import Enum
from typing import Any

from pydantic import BaseModel


class SchemaType(Enum):
    BASE = "base"
    CHAT = "chat"
    TEXT = "text"
    CUSTOM = "custom"


class BaseSchema(BaseModel):
    id: str = str(uuid.uuid4())
    type: SchemaType = SchemaType.BASE
    metadata: dict[str, Any] = {}
