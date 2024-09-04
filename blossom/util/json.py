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


def extract_markdown_first_json(text: str) -> str:
    if "```json" in text:
        text = text.split("```json")[1]
        text = text.split("```")[0]
    return text.strip()


def loads_markdown_first_json(text: str) -> dict[str, Any]:
    return json.loads(extract_markdown_first_json(text))
