from __future__ import annotations

from blossom.util.text import calculate_edit_distance, replace_text


def test_calculate_edit_distance() -> None:
    expected_distance = 3
    assert calculate_edit_distance("kitten", "sitting") == expected_distance
    assert calculate_edit_distance("", "") == 0
    assert calculate_edit_distance("a", "") == 1


def test_replace_text_case_sensitive() -> None:
    text = "Hello WORLD"
    assert replace_text(text, {"WORLD": "there"}) == "Hello there"


def test_replace_text_case_insensitive() -> None:
    text = "Hello WORLD"
    assert replace_text(text, {"world": "there"}, case_sensitive=False) == "Hello there"
