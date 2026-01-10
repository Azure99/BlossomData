from __future__ import annotations

from blossom.util.type import StrEnum


class _Example(StrEnum):
    A = "a"


def test_str_enum_str_and_repr() -> None:
    assert str(_Example.A) == "a"
    assert repr(_Example.A) == "_Example.A"
