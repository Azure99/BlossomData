import json
from typing import Any
import re

MARKDOWN_JSON_REGEX = re.compile(
    r"```(?:\s*json\s*)\n(.*?)\n```",
    re.IGNORECASE | re.DOTALL,
)


def extract_markdown_first_json(text: str) -> str:
    match = MARKDOWN_JSON_REGEX.search(text)
    if match:
        return match.group(1).strip()
    return text.strip()


def loads_markdown_first_json(text: str) -> dict[str, Any]:
    extracted = extract_markdown_first_json(text)
    try:
        result = json.loads(extracted)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON extracted from text. Extracted snippet:\n{extracted[:400]}"
        ) from e
    if not isinstance(result, dict):
        raise TypeError(f"Expected JSON object (dict), got {type(result).__name__}")
    return result


def loads_markdown_first_json_array(text: str) -> list[Any]:
    extracted = extract_markdown_first_json(text)
    try:
        result = json.loads(extracted)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid JSON extracted from text. Extracted snippet:\n{extracted[:400]}"
        ) from e
    if not isinstance(result, list):
        raise TypeError(f"Expected JSON array (list), got {type(result).__name__}")
    return result
