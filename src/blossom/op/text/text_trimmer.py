from typing import Optional

from blossom.op.map_operator import MapOperator
from blossom.schema.schema import Schema


class TextTrimmer(MapOperator):
    def __init__(
        self,
        strip_chars: Optional[str] = None,
    ):
        super().__init__()
        self.strip_chars = strip_chars

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_text(item)
        _item.content = _item.content.strip(self.strip_chars)
        return self._cast_base(_item)
