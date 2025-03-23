from blossom.op.filter_operator import FilterOperator
from blossom.schema.schema import Schema


class TextContentFilter(FilterOperator):
    def __init__(
        self,
        contents: list[str],
        case_sensitive: bool = True,
        reverse: bool = False,
    ):
        super().__init__(reverse=reverse)
        self.contents = contents
        self.case_sensitive = case_sensitive
        if not case_sensitive:
            self.contents = [content.lower() for content in contents]

    def process_item(self, item: Schema) -> bool:
        text = self._cast_text(item).content
        if not self.case_sensitive:
            text = text.lower()
        return all(content not in text for content in self.contents)
