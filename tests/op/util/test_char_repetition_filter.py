from __future__ import annotations

from blossom.op.util.char_repetition_filter import CharRepetitionFilter


def test_char_repetition_filter_short_text() -> None:
    op = CharRepetitionFilter(n=3)
    assert op.filter("ab") is True


def test_char_repetition_filter_repetition() -> None:
    op = CharRepetitionFilter(n=2, max_ratio=0.5)
    assert op.filter("abababab") is False
