import uuid
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class SchemaType(Enum):
    BASE = "base"
    CHAT = "chat"
    TEXT = "text"
    CUSTOM = "custom"


class BaseSchema(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    failed: Optional[bool] = None
    type: SchemaType = SchemaType.BASE
    metadata: dict[str, Any] = {}
