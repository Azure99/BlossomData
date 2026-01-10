from __future__ import annotations

import pytest

from blossom.util.json import (
    extract_markdown_first_json,
    loads_markdown_first_json,
    loads_markdown_first_json_array,
)


def test_extract_markdown_first_json() -> None:
    text = 'prefix\n```json\n{"a": 1}\n```\nsuffix'
    assert extract_markdown_first_json(text) == '{"a": 1}'
    assert extract_markdown_first_json('  {"b": 2}  ') == '{"b": 2}'


def test_loads_markdown_first_json() -> None:
    text = '```json\n{"a": 1}\n```'
    assert loads_markdown_first_json(text) == {"a": 1}

    with pytest.raises(ValueError):
        loads_markdown_first_json("```json\n{bad}\n```")

    with pytest.raises(TypeError):
        loads_markdown_first_json("```json\n[1, 2]\n```")


def test_loads_markdown_first_json_array() -> None:
    text = "```json\n[1, 2, 3]\n```"
    assert loads_markdown_first_json_array(text) == [1, 2, 3]

    with pytest.raises(TypeError):
        loads_markdown_first_json_array('```json\n{"a": 1}\n```')
