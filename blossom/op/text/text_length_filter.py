from typing import Callable

from blossom.op.filter_operator import FilterOperator
from blossom.schema.base_schema import BaseSchema


class TextLengthFilter(FilterOperator):
    def __init__(
        self,
        len_func: Callable[[str], int] = len,
        max_len: int = 16384,
    ):
        super().__init__()
        self.len_func = len_func
        self.max_len = max_len

    def process_item(self, item: BaseSchema) -> bool:
        _item = self._cast_text(item)
        return self.len_func(_item.content) <= self.max_len