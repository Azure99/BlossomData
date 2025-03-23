from typing import Callable

from blossom.op.filter_operator import FilterOperator
from blossom.schema.schema import Schema


class TextLengthFilter(FilterOperator):
    def __init__(
        self,
        len_func: Callable[[str], int] = len,
        max_len: int = 16384,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.len_func = len_func
        self.max_len = max_len

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_text(item)
        return self.len_func(_item.content) <= self.max_len
