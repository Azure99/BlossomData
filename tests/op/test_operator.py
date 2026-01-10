from __future__ import annotations

import pytest

from blossom.op.operator import Operator


class DummyOperator(Operator):
    def process(self, dataframe):
        return dataframe


def test_operator_cast_helpers(
    sample_text_schema, sample_chat_schema, sample_custom_schema, sample_row_schema
) -> None:
    op = DummyOperator()

    assert op._cast_base(sample_text_schema) is sample_text_schema
    assert op._cast_base_list([sample_row_schema]) == [sample_row_schema]

    assert op._cast_chat(sample_chat_schema) is sample_chat_schema
    assert op._cast_chat_list([sample_chat_schema]) == [sample_chat_schema]

    assert op._cast_custom(sample_custom_schema) is sample_custom_schema
    assert op._cast_custom_list([sample_custom_schema]) == [sample_custom_schema]

    assert op._cast_text(sample_text_schema) is sample_text_schema
    assert op._cast_text_list([sample_text_schema]) == [sample_text_schema]

    with pytest.raises(AssertionError):
        op._cast_chat(sample_text_schema)
