import json
from enum import Enum
from typing import Any

from pydantic import BaseModel


def json_dumps(data: Any, **kwargs) -> str:
    class EnumEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, Enum):
                return obj.value
            if isinstance(obj, BaseModel):
                return obj.model_dump()
            return super().default(obj)

    default_kwargs: dict[str, Any] = {"ensure_ascii": False, "cls": EnumEncoder}
    default_kwargs.update(kwargs)

    return json.dumps(data, **default_kwargs)
