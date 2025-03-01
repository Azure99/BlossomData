from blossom.op.filter_operator import FilterOperator
from blossom.op.util.char_repetition_filter import CharRepetitionFilter
from blossom.schema.schema import Schema


class TextRepetitionFilter(FilterOperator):
    def __init__(
        self,
        n: int = 10,
        min_ratio: float = 0.0,
        max_ratio: float = 0.5,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.char_repetition_filter = CharRepetitionFilter(
            n=n, min_ratio=min_ratio, max_ratio=max_ratio
        )

    def process_item(self, item: Schema) -> bool:
        _item = self._cast_text(item)
        return self.char_repetition_filter.filter(_item.content)
