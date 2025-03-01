from blossom.op.map_operator import MapOperator
from blossom.schema.schema import Schema
from blossom.util.text import replace_text


class TextContentReplacer(MapOperator):
    def __init__(
        self,
        replacements: dict[str, str],
        case_sensitive: bool = True,
    ):
        super().__init__()
        self.replacements = replacements
        self.case_sensitive = case_sensitive

    def process_item(self, item: Schema) -> Schema:
        _item = self._cast_text(item)
        _item.content = replace_text(
            text=_item.content,
            replacements=self.replacements,
            case_sensitive=self.case_sensitive,
        )
        return self._cast_base(_item)
