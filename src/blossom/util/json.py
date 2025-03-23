import json
from typing import Any


def extract_markdown_first_json(text: str) -> str:
    if "```json" in text:
        text = text.split("```json")[1]
        text = text.split("```")[0]
    return text.strip()


def loads_markdown_first_json(text: str) -> dict[str, Any]:
    result = json.loads(extract_markdown_first_json(text))
    assert isinstance(result, dict)
    return result


def loads_markdown_first_json_array(text: str) -> list[Any]:
    result = json.loads(extract_markdown_first_json(text))
    assert isinstance(result, list)
    return result
