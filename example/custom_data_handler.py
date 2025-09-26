from blossom.dataframe.data_handler import DataHandler
from blossom.dataset.loader import load_dataset
from blossom.op.text.text_content_filter import TextContentFilter
from blossom.schema.text_schema import TextSchema


# 自定义 DataHandler，明确数据字典与 TextSchema 之间的字段映射。
class CustomDataHandler(DataHandler):
    def from_dict(self, data):
        return TextSchema(content=data["your_text_field"])

    def to_dict(self, schema):
        return {"your_text_field": schema.content}


ops = [TextContentFilter(contents="bad_word")]

dataset = load_dataset(path="your_data.json", data_handler=CustomDataHandler())
dataset.execute(ops).write_json(
    "filtered_data.json",
    data_handler=CustomDataHandler(),
)
